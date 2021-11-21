# Bully
n this lab we will expand on Lab1 and join a group. This group will be fully interconnected (like a round table) and we will endeavor to choose a leader using the bully algorithm (Links to an external site.) (textbook pp. 330-332). Choosing a leader is a form of consensus.

You will program a node in the group in Python 3.

It will:

Have an identity which is the pair: (days until your next birthday, your SU ID)
JOIN the group by talking to the GCD.
Participate in elections.
Notice when the current leader has failed and initiate an election. (Extra Credit)
Pretend to fail every once in a while and then subsequently recover. (Extra Credit)
Details:

The "least" of two identities is the one with fewer days until the birthday, or if they have the same birthday, then the one with the smaller SU ID.
A process listens for other members wanting to send messages to it.
A process connects to each higher process by sending an ELECTION message to the higher process's listening server as described in the message protocol below.
Detecting a failed leader is done by each process sending occasional PROBE messages to her.
All messages are pickled and are a pair (message_name, message_data), where message_name is the text of the message name (that is, one of 'JOIN', 'ELECTION', 'COORDINATOR', or 'PROBE') and the message_data is specified in the protocol below or, if there is no message data, use None. Message responses, when they exist, can either be just pickled text or data as specified in the protocol below.
Sockets are not re-used. Once a message and its response, if it has one, are finished, the socket is closed. (For a commercial application, we would want to keep the sockets to avoid the reconnection overhead.)
Create your listening server with address ('localhost', 0) and then use listener.getsockname() to report your listener to the GCD when joining the group. The 'localhost' host name is special and usually translates to 127.0.0.1 and the port number of zero asks the socket library to allocate any free port for you.
Peer sockets must be non-blocking, i.e. you mustn't block waiting for the receipt of the OK when sending an ELECTION message (think about why this would cause our peers to think we had failed). Instead you need to poll for the response in conjunction with everything else you are simultaneously doing. It is recommended that you use socket.select() to do this.
Protocol
JOIN Message
When starting up, contact the GCD and send a JOIN message, which is a double: (process_id, listen_address) and whose response is a list of all the other members (some of which may now be failed). The message content's process_id is the identity pair (days_to_birthday, SU ID) and the listen_address is the pair (host, port) of your listening server. The returned data structure is a dictionary of all group members, keyed listen_address with values of corresponding process_id. You then start an election.

ELECTION Message
You initiate an election when you first join the group or whenever you notice the leader has failed. The ELECTION message is sent to each member with a higher process id than your own. If you are the highest process id in the group or if none of the higher processes respond within the given time limit, you win the election. If you win an election, you immediately send a COORDINATOR message to everyone. While you are waiting for responses from higher processes, you put yourself into an election-in-progress state.

The ELECTION message is a list of all the current (alive or failed) group members, including yourself. This is the same format as the list returned from the JOIN message.

When you receive an ELECTION message, you update your membership list with any members you didn't already know about, then you respond with the text OK. If you are currently in an election, that's all you do. If you aren't in an election, then proceed as though you are initiating a new election.

COORDINATOR Message
The COORDINATOR message is sent by the group leader when she wins an election. The message is the current list of processes in the group (without regard to if they are currently working or failed). The format is the same as that returned by the JOIN message. There is no response to a COORDINATOR message.

If you receive a COORDINATOR message, then change your state to not be election-in-progress and update your group membership list as necessary. Note the (possibly) new leader.

PROBE Message 
The PROBE message is sent occasionally to the group leader by all the other members of the group. There is no message data. The response is the text OK. Between each PROBE message, choose a random amount of time between 500 and 3000ms. This will require adaptation of the specification above to know your group leader from the COORDINATOR message--suggested way is for new leader to exclude all members with pid>himself in message payload.

Feigning Failure
At startup and whenever you recover from a previous feigned failure, start a timer and when the timer goes off, pretend you have failed, then eventually recover and go back to normal operation. The time to your next failure should be chosen each time randomly between 0 and 10000ms and the length of your failure should be chosen independently between 1000 and 4000ms. You can feign failure by either closing all your sockets, including the listening port or by just ignoring all incoming messages. Recovery is done by starting your listening server and initiating an election.
