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
int MAX_PEERS = 4;

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
void processPeerMessage(const string& message, const string& messageType);
bool isPortActive(int port);

int main(int argc, char* argv[]) {
    if (argc != 4) {
        cerr << "Usage: <processID> start <nProcesses> <portNo>" << endl;
        return 1;
    }
    int nProcesses = stoi(argv[2]);
    MAX_PEERS = nProcesses;
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

    while(true){
        sockaddr_in clientAddr{};
        socklen_t clientAddrSize = sizeof(clientAddr);
        int clientSock = accept(serverSock, (struct sockaddr*)&clientAddr, &clientAddrSize);
        if (clientSock < 0) {
            cerr << "Error accepting connection." << endl;
            exit(1);
        }
        thread clientThead(handleConnection, clientSock);
        clientThead.detach();
    }
}

void handleConnection(int sock) {
    char buffer[MSG_SIZE];
    while(true){
        memset(buffer, 0, MSG_SIZE); // Initialize the buffer to zero

        // Attempt to receive data from the socket
        ssize_t bytesReceived = recv(sock, buffer, MSG_SIZE - 1, 0);
        if (bytesReceived <= 0) {
            continue;
        }

        // Convert the received data into a string for easier processing
        string receivedData(buffer, bytesReceived);
        cout << serverIdentifier << " receive: " <<  receivedData << endl;
        // Inside handleConnection
        if (receivedData.find("get chatLog") == 0) {
            processProxyCommand(sock, "get chatLog");
        } else if (receivedData.find("crash") == 0) {
            processProxyCommand(sock, "crash");
        } else if (receivedData.find("RUMOR") == 0) {
            processPeerMessage(receivedData, "RUMOR");
            break;
        } else if (receivedData.find("STATUS") == 0) {
            processPeerMessage(receivedData, "STATUS");
            break;
        } else if (receivedData.find("msg") == 0) {
            processNewClientMessage(sock, receivedData);
        }
    }
    close(sock);
}

void processNewClientMessage(int sock, const string& receivedData) {
    stringstream ss(receivedData);
    string messageType, messageID, messageText;
    getline(ss, messageType, ' ');
    getline(ss, messageID, ' ');
    getline(ss, messageText);

    lock_guard<mutex> guard(logsMutex);

    string origin = serverIdentifier;
    int seqNo = ++maxSeqNos[origin];
    chatLogs[origin].emplace_back(Message{origin, seqNo, messageText});

    Message msg{origin, seqNo, messageText};
    forwardMessage(msg, stoi(serverIdentifier));
}

void processProxyCommand(int sock, const string& command) {
    if (command == "get chatLog") {
        // lock_guard<mutex> guard(logsMutex);
        stringstream chatLogStream;
        chatLogStream << "chatLog ";
        vector<string> v_message;
        for (const auto& [origin, messages] : chatLogs) {
            for (const auto& message : messages) {
                v_message.push_back(message.text);
            }
        }
        for(int i = 0; i < v_message.size(); i++){
            chatLogStream << v_message[i];
            if(i != v_message.size() - 1){
                chatLogStream << ",";
            }
        }
        chatLogStream << '\n';
        string chatLog = chatLogStream.str();
        // Send back to proxy
        send(sock, chatLog.c_str(), chatLog.length(), 0);
        cout << "return chatLog to proxy." << endl;
    } else if (command == "crash") {
        exit(0);
    }
}

void processPeerMessage(const string& message, const string& messageType) {
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
        if (maxSeqNos.find(origin) == maxSeqNos.end()){
            chatLogs[origin] = vector<Message>{};
            maxSeqNos[origin] = 0;
        }
        if (maxSeqNos[origin] < seqNo) {
            // Update chat log and max sequence number
            chatLogs[origin].push_back({origin, seqNo, text});
            maxSeqNos[origin] = seqNo;

            // Forward the message to another peer
            forwardMessage({origin, seqNo, text}, stoi(serverIdentifier));
        }
    } else if (messageType == "STATUS") {
        bool sentStatus = false;
        bool sentRumor = false;
        // Process the status message
        // Respond with any messages the sender is missing
        stringstream ss(message);
        string dummy, peerOrigin;
        int peerSeqNo;

        ss >> dummy;
        unordered_map<string, int> peerStatus;
        while(ss >> peerOrigin >> peerSeqNo){
            peerStatus[peerOrigin] = peerSeqNo;
        }
        for(const auto& [origin, seqNo]: maxSeqNos){
            if(peerStatus.find(origin) == peerStatus.end() || peerStatus[origin] < seqNo){
                for(const auto& msg : chatLogs[origin]){
                    if(msg.seqNo >= peerStatus[origin]){
                        int destPort = getRandomNeighborPort(stoi(serverIdentifier));
                        sentRumor = true;
                        sendMessage(destPort, "RUMOR " + msg.origin + " " + to_string(msg.seqNo) + "  " + msg.text);
                    }
                }
            }
        }
        for(const auto& [peerOrigin, peerSeqNo] : peerStatus){
            if(maxSeqNos[peerOrigin] < peerSeqNo){
                sentStatus = true;
                int destPort = getRandomNeighborPort(stoi(serverIdentifier));
                sendMessage(destPort, compileStatusMessage());
                break;
            }
        }
        if(!sentRumor && sentStatus){
            if(flipCoin()){
                int destPort = getRandomNeighborPort(stoi(serverIdentifier));
                if(destPort != -1){
                    sendMessage(destPort, compileStatusMessage());
                }
                else{
                    cout << "No valid neighbors to continue rumormongering." << endl;
                }
            }
            else{
                cout << "Ceasing rumormongering process." << endl;
            }
        }
    }
}

void forwardMessage(const Message& msg, int currPort) {
    int destPort = getRandomNeighborPort(currPort);
    cout << "send to neighbor: " << destPort << endl;
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

        int destPort = getRandomNeighborPort(stoi(serverIdentifier));

        // Compile a status message
        string statusMessage = compileStatusMessage();

        // Send the status message to a randomly selected peer
        if (destPort != -1) {
            sendMessage(destPort, statusMessage);
        }
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
        close(sock); // Close the socket if unaFull Yearble to connect
        return;
    }

    // Send the message
    if (send(sock, msg.c_str(), msg.length(), 0) < 0) {
        cerr << "Error sending message to port: " << destPort << endl;
    }

    // Close the socket after sending the message
    close(sock);
    cout << "send " << msg << " to " << destPort << endl;
}

int getRandomNeighborPort(int currentPort) {
    vector<int> possibleNeighbors;

    // Check if the previous port is active and valid
    if (currentPort > BASE_PORT && isPortActive(currentPort - 1)) {
        possibleNeighbors.push_back(currentPort - 1);
    }

    // Check if the next port is active and valid
    if (currentPort < BASE_PORT + SERVER_COUNT - 1 && isPortActive(currentPort + 1)) {
        possibleNeighbors.push_back(currentPort + 1);
    }

    // Now, randomly select a neighbor from the possible (and active) options
    if (!possibleNeighbors.empty()) {
        srand(time(0) + currentPort); // Use currentPort to seed for variety per process
        int randomIndex = rand() % possibleNeighbors.size();
        return possibleNeighbors[randomIndex];
    }

    return -1; // Indicate an error or no valid (active) neighbors
}


bool flipCoin() {
    std::random_device rd; // Obtain a random number from hardware
    std::mt19937 gen(rd()); // Seed the generator
    std::bernoulli_distribution d(0.5); // Define a distribution with a 50/50 chance

    return d(gen); // Returns true for "heads", false for "tails"
}

bool isPortActive(int port) {
    int testSock = socket(AF_INET, SOCK_STREAM, 0);
    if (testSock < 0) {
        cerr << "Error creating socket for testing port activity." << endl;
        return false;
    }

    sockaddr_in testAddr{};
    testAddr.sin_family = AF_INET;
    testAddr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &testAddr.sin_addr);

    // Attempt to connect to the port
    bool active = connect(testSock, (sockaddr*)&testAddr, sizeof(testAddr)) >= 0;
    close(testSock); // Close the socket after testing
    return active;
}
