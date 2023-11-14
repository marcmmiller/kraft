package kraft

import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.cio.*
import io.ktor.server.http.*
import io.ktor.server.plugins.callloging.CallLogging
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
//import kotlin.concurrent.currentThread
import kotlin.math.min
import kotlin.random.Random
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.serialization.*
//import kotlinx.serialization.json.*

@Serializable
sealed class RpcRequest(val rpcName : String)

class RpcClient {
    val httpClient = HttpClient(io.ktor.client.engine.cio.CIO) {
        install(io.ktor.client.plugins.contentnegotiation.ContentNegotiation) {
            json()
        }
    }


    inline suspend fun <reified Req: RpcRequest, reified Resp> sendRpc(dest : Int,
                                                               req : Req) : Resp? {
        try {
            val r = httpClient.post("http://localhost:$dest/${req.rpcName}") {
                this.contentType(ContentType.Application.Json)
                setBody(req)
            }
            return r.body()
        }
        catch (e: java.net.ConnectException) {
            println("Connect exception: cannot connect to $dest")
            return null
        }
    }
}


// Raft has two RPC types: RequestVote and AppendEntries
@Serializable
data class RequestVoteArgs(val term : Int,
                           val candidateId : Int,
                           val lastLogIndex : Int = 0,
                           val lastLogTerm : Int = 0
) : RpcRequest("RequestVote")

@Serializable
data class RequestVoteResponse(val voteGranted : Boolean,
                               val term : Int,
                               val followerId : Int
)

@Serializable
data class LogEntry(val term : Int,
                    val key : String,
                    val value : String)


@Serializable
data class AppendEntriesArgs(val term : Int,
                             val leaderId : Int,
                             val prevLogIndex : Int,
                             val prevLogTerm : Int,
                             val entries : Array<LogEntry>,
                             val leaderCommit : Int
                             
) : RpcRequest("AppendEntries")

@Serializable
data class AppendEntriesResponse(val success : Boolean,
                                 val term : Int)


class SimpleDb {
    val dict = mutableMapOf<String, String>()
    fun commit(entry : LogEntry) {
        dict[entry.key] = entry.value
    }

    fun getVal(key: String) : String? {
        return dict[key]
    }

    override fun toString() : String {
        return dict.toString()
    }
}


class Raft(val selfId : Int, val replicas : Array<Int>) {
    enum class RaftState {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    val db = SimpleDb()
    val rpcClient = RpcClient()
    
    var state = RaftState.FOLLOWER

    // true if we're a follower and we've received a heartbeat in the election interval
    var followerReceivedHeartbeat = false
    var currentTerm = 0
    var currentLeader = -1

    val log = arrayListOf<LogEntry>()

    // Index of highest log entry known to be committed (1-based) by the leader
    var commitIndex = 0

    // Index of highest log entry applied to the local state machine (1-based)
    var lastApplied = 0

    // When leader, the index of the next log entry to send to each server (1-based)
    var nextIndex = mutableMapOf<Int, Int>()

    // When leader, index of highest log entry known to be replicated to that servcer (1-based)
    var matchIndex = mutableMapOf<Int, Int>()

    val isLeader get() = selfId == currentLeader
    val leader get() = currentLeader

    suspend fun put(k: String, v: String) : Boolean {
        assert(state == RaftState.LEADER)
        log.add(LogEntry(currentTerm, k, v))

        println(" log is now ${log.size} long and nextIndex is $nextIndex ")
        
        // TODO: sendAppendEntries should return whether replication was
        // successful on a majority of servers
        sendAppendEntries()
        return true
    }

    fun getVal(k: String) : String? {
        return db.getVal(k)
    }

    suspend fun requestVotes() = coroutineScope {
        currentTerm++
        state = RaftState.CANDIDATE
        println(" $selfId requesting votes in term $currentTerm")
        val rpcArgs = RequestVoteArgs(currentTerm, selfId)
        var nVotes = 1
        val voteChannel = Channel<RequestVoteResponse?>()
        for (r in replicas) {
            launch {
                val response = rpcClient.sendRpc<RequestVoteArgs, RequestVoteResponse>(r, rpcArgs)
                voteChannel.trySend(response)
            }
        }

        var nResponses = 0
        while (nResponses < replicas.size) {
            val resp = voteChannel.receive()
            nResponses++
            println(" $selfId nResponses = $nResponses ")

            if (resp != null && resp.voteGranted) {
                ++nVotes;
                println(" $selfId received vote from ${resp.followerId} ")
            }
            if (state == RaftState.FOLLOWER) {
                println(" $selfId aborted candidacy")
                // Another leader interrupted my candidacy
                break
            }
            if (nVotes > ((replicas.size + 1) / 2)) {
                println("** $selfId has been elected leader of term $currentTerm")
                state = RaftState.LEADER
                currentLeader = selfId
                
                // Initialize leader replication state
                nextIndex = mutableMapOf<Int, Int>()
                matchIndex = mutableMapOf<Int, Int>()
                for (r in replicas) {
                    nextIndex[r] = log.size + 1
                    matchIndex[r] = 0
                }

                break
            }
        }

        voteChannel.close()

        if (state != RaftState.LEADER) {
            println(" $selfId failed to be elected")
            state = RaftState.FOLLOWER
            followerReceivedHeartbeat = false
        }
    }

    fun onRequestVote(args : RequestVoteArgs) : RequestVoteResponse {
        if (args.term > currentTerm) {
            state = RaftState.FOLLOWER
            currentTerm = args.term
            currentLeader = -1  // we will update the leader on the AppendEntries call
            // TODO: don't vote for the candidate if I have a longer log in the present term
            println(" $selfId voting 'yes' on ${args.candidateId} for term $currentTerm")
            return RequestVoteResponse(true, currentTerm, selfId)
        }
        else {
            println(" $selfId voting 'no' on ${args.candidateId}")
            return RequestVoteResponse(false, currentTerm, selfId)
        }
    }

    suspend fun sendAppendEntries() {
        var nSuccess = 0

        // TODO: send these in parallel, once a majority is replicated then commit entries
        for (r in replicas) {
            var rpcArgs : AppendEntriesArgs
            var nextIndex = nextIndex[r] ?: 0
            
            if (nextIndex > log.size) {
                rpcArgs = AppendEntriesArgs(currentTerm, selfId, 0, 0, arrayOf<LogEntry>(), commitIndex)
            }
            else {
                println("Sending actual entries")
                // send log[nextIndex - 1] ... log.size()
                // TODO: try using a sublist and changing spec to a collection
                val logEntries = log.subList(nextIndex - 1, log.size).toTypedArray()

                val prevLogIndex = nextIndex - 1
                val prevLogTerm = if (prevLogIndex > 0) log[prevLogIndex - 1].term else 0

                // send log STARTING WITH nextIndex and if that fails then decrement
                // nextIndex.  Afterwards, update nextIndex and matchIndex
                rpcArgs = AppendEntriesArgs(currentTerm, selfId, prevLogIndex, prevLogTerm, logEntries, commitIndex)
            }

            val resp = rpcClient.sendRpc<AppendEntriesArgs, AppendEntriesResponse>(r, rpcArgs)
            if (resp == null) {
                println("Unsuccessful receipt of AppendEntries from $r")
            }
            else if (!resp.success) {
                println("NOT YET IMPLEMENTED: conflict resolution needed")
            }
            else {
                if (nextIndex != log.size + 1) {
                    println("[ni for $r: ${nextIndex} -> ${log.size + 1}]");
                }

                this.nextIndex[r] = log.size + 1
                ++nSuccess;
            }
        }

        if ((nSuccess + 1) > (((replicas.size + 1) / 2) + 1)) {
            commitIndex = log.size
        }
    }
    
    fun onAppendEntries(args : AppendEntriesArgs) : AppendEntriesResponse {
        var success = true
        followerReceivedHeartbeat = true
        if (args.term < currentTerm) {
            println(" $selfId received AppendEntries for old term $args")
            success = false
        }
        else if (state == RaftState.FOLLOWER) {
            print("[$selfId]")
            success = true
        }
        else if (args.term >= currentTerm) {
            // There shouldn't be more than one leader.
            assert(state == RaftState.CANDIDATE);
            println(" $selfId interruped as candidate for term $currentTerm by AppendEntries $args")
            success = true
        }

        if (success) {
            state = RaftState.FOLLOWER
            currentTerm = args.term
            currentLeader = args.leaderId
            if (args.entries.size > 0) {
                if ((args.prevLogIndex <= 0)
                        || (log.size >= args.prevLogIndex && log[args.prevLogIndex - 1].term == args.prevLogTerm)) {
                    // no conflicts with the leader -- append the entries, truncating log if necessary
                    log.subList(args.prevLogIndex, log.size).clear()
                    log.addAll(args.entries)
                    println(" $selfId appended to log: (log.size = ${log.size}) ")
                }
                else {
                    // TODO: How do we recover from this state?
                    println(" $selfId (follower) received conflicting appendEntries ")
                    success = false;
                }
            }
        }

        if (success) {
            if (args.leaderCommit > commitIndex) {
                commitIndex = min(args.leaderCommit, log.size)
                println(" $selfId new commitIndex is $commitIndex (log.size is ${log.size}")
            }
        }

        return AppendEntriesResponse(success, currentTerm)
    }

    var inHeartBeat = false

    suspend fun maybeTriggerElection() {
        if (state == RaftState.FOLLOWER && !followerReceivedHeartbeat) {
            println(" $selfId is triggering election due to election timeout")
            requestVotes();
            if (state == RaftState.LEADER) {
                println("Leader $selfId sending heartbeat")
                sendHeartbeat()
            }
        }
    }

    suspend fun sendHeartbeat() {
        print("<3")
        sendAppendEntries()
    }

    suspend fun run() {
        while (true) {
            // All servers:
            while (commitIndex > lastApplied) {
                ++lastApplied
                db.commit(log[lastApplied - 1])
                println(" $selfId database: $db ")
            }
            
            if (state == RaftState.FOLLOWER) {
                delay(Random.nextInt(300, 600).toLong())
                maybeTriggerElection()
            }
            else if (state == RaftState.LEADER) {
                delay(50)
                if (state == RaftState.LEADER) {
                    sendHeartbeat()
                }
            }
            else {
                delay(50)
            }
        }
    }
}

class App(val raft : Raft) {
    val dict = mutableMapOf<String, String>()

    fun startServer(scope : CoroutineScope, port : Int) {
        scope.embeddedServer(io.ktor.server.cio.CIO, port = port) {
            install(io.ktor.server.plugins.contentnegotiation.ContentNegotiation) {
                json()
            }
            install(CallLogging)
            routing {
                get("/") {
                    call.respondText("OK")
                };
                post("/RequestVote") {
                    call.respond(raft.onRequestVote(call.receive<RequestVoteArgs>()));
                }
                post("/AppendEntries") {
                    call.respond(raft.onAppendEntries(call.receive<AppendEntriesArgs>()));
                };
                get("/put") {
                    val k = call.parameters.get("k");
                    val v = call.parameters.get("v");
                    if (k != null && v!= null) {
                        if (raft.isLeader) {
                            raft.put(k, v)
                            call.respondText("OK")
                        }
                        else {
                            call.respondRedirect("http://localhost:${raft.leader}/put?k=$k&v=$v")
                        }
                    }
                    else {
                        call.respond(HttpStatusCode.BadRequest, "Missing params.")
                    }
                };
                get("/get") {
                    val k = call.parameters.get("k")
                    if (k != null) {
                        if (raft.isLeader) {
                            val v = raft.getVal(k)
                            if (v != null) {
                                call.respondText(v)
                            }
                            else {
                                call.respond(HttpStatusCode.NotFound)
                            }
                        }
                    }
                    else {
                        call.respond(HttpStatusCode.BadRequest, "Missing key.")
                    }
                }
            }
        }.start(wait = false)

        println("Listening on port $port")
    }

    suspend fun run() {
        raft.run()
    }
    
}


fun main(args : Array<String>) {
    val port = args[0].toInt()
    val replicas = arrayOf(args[1].toInt(), args[2].toInt())
    
    runBlocking {
        val r = Raft(port, replicas)
        val a = App(r)
        a.startServer(this, port)

        a.run()
    }
}
