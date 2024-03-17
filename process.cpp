#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <sstream>
#include <thread>
#include <mutex>
#include <chrono>
#include <random>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

using namespace std;

const int BASE_PORT = 20000; // Base port number for server-server communication
const size_t MSG_SIZE = 1024; // Maximum message size
const int MAX_PEERS = 4;

// Message struct for storing chat messages
struct Message {
    string origin;
    int seqNo;
    string text;
};

// Global data structures
unordered_map<string, vector<Message>> chatLogs; // Chat logs separated by origin
unordered_map<string, int> maxSeqNos; // Highest sequence number seen for each origin
mutex logsMutex; // Mutex for synchronizing access to chatLogs and maxSeqNos
std::string serverIdentifier;

// Function declarations
void processProxyCommand(int sock, const string& command);
void processPeerMessage(int sock, const string& message, const string& messageType);
void forwardMessage(const Message& msg, int currPort);
void respondToStatus(int sock, const string& statusMessage);
string compileStatusMessage();
void handleConnection(int sock);
void antiEntropy();
void sendMessage(int destPort, const string& msg);
void startServer(int port);
int getRandomNeighborPort(int currentPort);
bool flipCoin();
void processNewClientMessage(int sock, const string& receivedData);


int main(int argc, char* argv[]) {
    if (argc != 4) {
        cerr << "Usage: " << argv[0] << " <processID> <nProcesses> <portNo>" << endl;
        return 1;
    }

    int portNo = stoi(argv[3]);
    // Start anti-entropy process
    thread antiEntropyThread(antiEntropy);
    antiEntropyThread.detach();
    serverIdentifier = std::to_string(portNo);
    // Start server
    startServer(portNo);

    return 0;
}

void startServer(int port) {
    int serverSock = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSock < 0) {
        cerr << "Error creating socket." << endl;
        exit(1);
    }

    sockaddr_in serverAddr{};
    memset(&serverAddr, 0, sizeof(serverAddr)); // Ensure struct is empty
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(port);
    serverAddr.sin_addr.s_addr = INADDR_ANY;

    // Use ::bind to specify the global namespace, avoiding ambiguity with std::bind
    if (::bind(serverSock, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        cerr << "Error binding socket." << endl;
        close(serverSock);
        exit(1);
    }

    if (listen(serverSock, 10) < 0) { // Listen for incoming connections, maximum 10 pending connections
        cerr << "Error listening on socket." << endl;
        close(serverSock);
        exit(1);
    }

    cout << "Server started on port " << port << endl;

    while (true) {
        sockaddr_in clientAddr{};
        socklen_t clientAddrSize = sizeof(clientAddr);
        int clientSock = accept(serverSock, (struct sockaddr*)&clientAddr, &clientAddrSize);
        if (clientSock < 0) {
            cerr << "Error accepting connection." << endl;
            continue;
        }
        thread(handleConnection, clientSock).detach(); // Start a new thread to handle the connection
    }
}

void handleConnection(int sock) {
    char buffer[MSG_SIZE];
    memset(buffer, 0, MSG_SIZE); // Initialize the buffer to zero

    // Attempt to receive data from the socket
    ssize_t bytesReceived = recv(sock, buffer, MSG_SIZE - 1, 0);
    if (bytesReceived <= 0) {
        cerr << "Error reading from socket or connection closed." << endl;
        close(sock); // Close the socket if an error occurs or if the connection is closed
        return;
    }

    // Convert the received data into a string for easier processing
    string receivedData(buffer, bytesReceived);

    // Inside handleConnection
    if (receivedData.find("get chatLog") == 0) {
        processProxyCommand(sock, "get chatLog");
    } else if (receivedData.find("crash") == 0) {
        processProxyCommand(sock, "crash");
    } else if (receivedData.find("RUMOR") == 0) {
        processPeerMessage(sock, receivedData, "RUMOR");
    } else if (receivedData.find("STATUS") == 0) {
        processPeerMessage(sock, receivedData, "STATUS");
    } else if (receivedData.find("msg") == 0) {
        processNewClientMessage(sock, receivedData);
    }


    close(sock); // Close the socket after processing the command or message
}

void processNewClientMessage(int sock, const string& receivedData) {
    // Assuming receivedData format for new message is "msg <Origin> <Text>"
    stringstream ss(receivedData);
    string messageType, origin, text;
    
    getline(ss, messageType, ' '); // Skip the "msg" part
    getline(ss, origin, ' '); // Get the origin (username or identifier)
    
    getline(ss, text); // The rest is the message text
    
    int newSeqNo;
    {
        lock_guard<mutex> guard(logsMutex); // Ensure thread safety when accessing shared data
        
        // If this server is the origin of the message, increment sequence number for this origin
        if (origin == serverIdentifier) { // Replace with actual check for server's identifier
            maxSeqNos[origin]++; // Increment sequence number for new message
            newSeqNo = maxSeqNos[origin];
        } else {
            // If the message is being forwarded and already has a sequence number, use that
            // This part assumes the receivedData includes the sequence number for forwarded messages
            // You'll need to adjust how you parse receivedData to extract the sequence number if forwarding
            newSeqNo = stoi(text.substr(0, text.find(' '))); // Example, adjust based on actual format
            text = text.substr(text.find(' ') + 1); // Adjust based on actual format
        }
        
        // Store the new message in the chat log
        chatLogs[origin].push_back({origin, newSeqNo, text});
    }
    
    // Forward the message to another peer
    forwardMessage({origin, newSeqNo, text}, sock);
}

void processProxyCommand(int sock, const string& command) {
    if (command == "get chatLog") {
        lock_guard<mutex> guard(logsMutex);
        stringstream chatLogStream;
        for (const auto& [origin, messages] : chatLogs) {
            for (const auto& message : messages) {
                chatLogStream << message.origin << ":" << message.seqNo << ":" << message.text << ",";
            }
        }
        string chatLog = chatLogStream.str();
        // Send back to proxy
        send(sock, chatLog.c_str(), chatLog.length(), 0);
    } else if (command == "crash") {
        exit(0);
    }
}

void processPeerMessage(int sock, const string& message, const string& messageType) {
    // Lock mutex for thread safety
    lock_guard<mutex> guard(logsMutex);

    if (messageType == "RUMOR") {
        stringstream ss(message);
        string dummy, origin, text;
        int seqNo;
        ss >> dummy >> origin >> seqNo;
        getline(ss, text);
        text = text.substr(1); // Remove leading space

        // Check if this is a new message
        if (maxSeqNos[origin] < seqNo) {
            // Update chat log and max sequence number
            chatLogs[origin].push_back({origin, seqNo, text});
            maxSeqNos[origin] = seqNo;

            // Forward the message to another peer
            forwardMessage({origin, seqNo, text}, sock);
        }
    } else if (messageType == "STATUS") {
        // Process the status message
        // Respond with any messages the sender is missing
        respondToStatus(sock, message);
    }
}

void forwardMessage(const Message& msg, int currPort) {
    int destPort = getRandomNeighborPort(currPort);

    // Convert the message to string format
    string messageString = "RUMOR " + msg.origin + " " + to_string(msg.seqNo) + " " + msg.text;
    
    // Send the message
    sendMessage(destPort, messageString);
}

void respondToStatus(int sock, const string& statusMessage) {
    lock_guard<mutex> guard(logsMutex); // Lock for thread safety
    
    bool shouldSendOwnStatus = false;
    bool sentRumor = false;
    stringstream ss(statusMessage);
    string dummy, token;
    getline(ss, dummy, ' '); // Skip the STATUS word

    unordered_map<string, int> peerStatus; // Stores the status message in a map for easy lookup

    // Parse the status message
    while (getline(ss, token, ' ')) {
        size_t delimPos = token.find(":");
        if (delimPos != string::npos) {
            string origin = token.substr(0, delimPos);
            int seqNo = stoi(token.substr(delimPos + 1));
            peerStatus[origin] = seqNo;
        }
    }

    // Determine messages to send based on comparison
    for (const auto& [origin, seqNo] : maxSeqNos) {
        auto it = peerStatus.find(origin);
        if (it == peerStatus.end() || it->second < seqNo) {
            // The peer is missing at least one message from this origin.
            for (const auto& msg : chatLogs[origin]) {
                if (msg.seqNo >= (it == peerStatus.end() ? 1 : it->second)) {
                    // Send messages the peer is missing
                    string messageString = "RUMOR " + msg.origin + " " + to_string(msg.seqNo) + " " + msg.text;
                    sendMessage(sock, messageString);
                    sentRumor = true;
                }
            }
        }
    }

    // Check if we are missing messages
    for (const auto& [origin, seqNo] : peerStatus) {
        if (maxSeqNos[origin] < seqNo) {
            shouldSendOwnStatus = true;
            break;
        }
    }

    if (shouldSendOwnStatus) {
        string myStatus = compileStatusMessage();
        sendMessage(sock, myStatus); // Assume this sends the status message back to the peer
    }
    if (!sentRumor && !shouldSendOwnStatus) {
        if (flipCoin()) {
            int newNeighborPort = getRandomNeighborPort(sock);
            if (newNeighborPort != -1) {
                string myStatus = compileStatusMessage();
                sendMessage(newNeighborPort, myStatus); // Send your status to start rumormongering with the new neighbor
            } else {
                std::cout << "No valid neighbors to continue rumormongering." << std::endl;
            }
        } else {
            // Tails: Cease the rumormongering process
            std::cout << "Ceasing rumormongering process." << std::endl;
        }
    }
}

void antiEntropy() {
    while (true) {
        this_thread::sleep_for(chrono::seconds(10)); // Adjust timing as needed

        int peerIndex = rand() % MAX_PEERS;
        int destPort = BASE_PORT + peerIndex;

        // Compile a status message
        string statusMessage = compileStatusMessage();

        // Send the status message to a randomly selected peer
        sendMessage(destPort, statusMessage);
    }
}

string compileStatusMessage() {
    lock_guard<mutex> guard(logsMutex);
    string status = "STATUS";
    for (const auto& [origin, seqNo] : maxSeqNos) {
        status += " " + origin + ":" + to_string(seqNo);
    }
    return status;
}

void sendMessage(int destPort, const string& msg) {
    // Create a socket
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        cerr << "Error creating socket for sending message." << endl;
        return;
    }

    // Set up the destination address structure
    sockaddr_in destAddr{};
    destAddr.sin_family = AF_INET;
    destAddr.sin_port = htons(destPort);
    inet_pton(AF_INET, "127.0.0.1", &destAddr.sin_addr);

    // Connect to the destination
    if (connect(sock, (sockaddr*)&destAddr, sizeof(destAddr)) < 0) {
        cerr << "Error connecting to destination port: " << destPort << endl;
        close(sock); // Close the socket if unable to connect
        return;
    }

    // Send the message
    if (send(sock, msg.c_str(), msg.length(), 0) < 0) {
        cerr << "Error sending message to port: " << destPort << endl;
    }

    // Close the socket after sending the message
    close(sock);
}

int getRandomNeighborPort(int currentPort) {
    vector<int> possibleNeighbors;

    // Check if the previous port is valid
    if (currentPort > BASE_PORT) {
        possibleNeighbors.push_back(currentPort - 1);
    }

    // Check if the next port is valid
    if (currentPort < BASE_PORT + MAX_PEERS - 1) {
        possibleNeighbors.push_back(currentPort + 1);
    }

    // Now, randomly select a neighbor from the possible options
    if (!possibleNeighbors.empty()) {
        srand(time(0) + currentPort); // Use currentPort to seed for variety per process
        int randomIndex = rand() % possibleNeighbors.size();
        return possibleNeighbors[randomIndex];
    }

    return -1; // Indicate an error or no valid neighbors
}

bool flipCoin() {
    std::random_device rd; // Obtain a random number from hardware
    std::mt19937 gen(rd()); // Seed the generator
    std::bernoulli_distribution d(0.5); // Define a distribution with a 50/50 chance

    return d(gen); // Returns true for "heads", false for "tails"
}
