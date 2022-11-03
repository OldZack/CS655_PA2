import java.util.*;
import java.io.*;

public class StudentNetworkSimulator extends NetworkSimulator
{
    /*
     * Predefined Constants (static member variables):
     *
     *   int MAXDATASIZE : the maximum size of the Message data and
     *                     Packet payload
     *
     *   int A           : a predefined integer that represents entity A
     *   int B           : a predefined integer that represents entity B
     *
     * Predefined Member Methods:
     *
     *  void stopTimer(int entity):
     *       Stops the timer running at "entity" [A or B]
     *  void startTimer(int entity, double increment):
     *       Starts a timer running at "entity" [A or B], which will expire in
     *       "increment" time units, causing the interrupt handler to be
     *       called.  You should only call this with A.
     *  void toLayer3(int callingEntity, Packet p)
     *       Puts the packet "p" into the network from "callingEntity" [A or B]
     *  void toLayer5(String dataSent)
     *       Passes "dataSent" up to layer 5
     *  double getTime()
     *       Returns the current time in the simulator.  Might be useful for
     *       debugging.
     *  int getTraceLevel()
     *       Returns TraceLevel
     *  void printEventList()
     *       Prints the current event list to stdout.  Might be useful for
     *       debugging, but probably not.
     *
     *
     *  Predefined Classes:
     *
     *  Message: Used to encapsulate a message coming from layer 5
     *    Constructor:
     *      Message(String inputData):
     *          creates a new Message containing "inputData"
     *    Methods:
     *      boolean setData(String inputData):
     *          sets an existing Message's data to "inputData"
     *          returns true on success, false otherwise
     *      String getData():
     *          returns the data contained in the message
     *  Packet: Used to encapsulate a packet
     *    Constructors:
     *      Packet (Packet p):
     *          creates a new Packet that is a copy of "p"
     *      Packet (int seq, int ack, int check, String newPayload)
     *          creates a new Packet with a sequence field of "seq", an
     *          ack field of "ack", a checksum field of "check", and a
     *          payload of "newPayload"
     *      Packet (int seq, int ack, int check)
     *          chreate a new Packet with a sequence field of "seq", an
     *          ack field of "ack", a checksum field of "check", and
     *          an empty payload
     *    Methods:
     *      boolean setSeqnum(int n)
     *          sets the Packet's sequence field to "n"
     *          returns true on success, false otherwise
     *      boolean setAcknum(int n)
     *          sets the Packet's ack field to "n"
     *          returns true on success, false otherwise
     *      boolean setChecksum(int n)
     *          sets the Packet's checksum to "n"
     *          returns true on success, false otherwise
     *      boolean setPayload(String newPayload)
     *          sets the Packet's payload to "newPayload"
     *          returns true on success, false otherwise
     *      int getSeqnum()
     *          returns the contents of the Packet's sequence field
     *      int getAcknum()
     *          returns the contents of the Packet's ack field
     *      int getChecksum()
     *          returns the checksum of the Packet
     *      int getPayload()
     *          returns the Packet's payload
     *
     */

    /*   Please use the following variables in your routines.
     *   int WindowSize  : the window size
     *   double RxmtInterval   : the retransmission timeout
     *   int LimitSeqNo  : when sequence number reaches this value, it wraps around
     */

    public static final int FirstSeqNo = 0;
    private int WindowSize;
    private double RxmtInterval;
    private int LimitSeqNo;

    // Statistic Variables
    private int originPktNum;
    private int retransPktNum;
    private int deliveredPktNum;
    private int ackPktNum;
    private int corruptPktNum;
    private int receivedPktNum;

    // Calculating RTT variables
    ArrayList<Double> RTTList;
    private HashMap<Integer, Double> sendTime;

    // Calculating communication time variables
    ArrayList<Double> CTList;
    private HashMap<Integer, Double> oriSendTime;



    // Add any necessary class variables here.  Remember, you cannot use
    // these variables to send messages error free!  They can only hold
    // state information for A or B.
    // Also add any necessary methods (e.g. checksum of a String)

    // Variables for the sender (A)
    // Stores the data that is not sent yet.
    private Queue<Packet> unsentBuffer_a;
    // Stores the data that is sent but might require resent.
    private Queue<Packet> resentBuffer_a;
    private int nextSeqNum_a;
    private int lastAckNum_a;
    private boolean timerFlag_a;
    private int currSeqNum_a;

    // Variables for the receiver (B)
    private HashMap<Integer,Packet> buffer_B;
    //window notation
    private int wanted_B;
    private int max_B;

    // This is the constructor.  Don't touch!
    public StudentNetworkSimulator(int numMessages,
                                   double loss,
                                   double corrupt,
                                   double avgDelay,
                                   int trace,
                                   int seed,
                                   int winsize,
                                   double delay)
    {
        super(numMessages, loss, corrupt, avgDelay, trace, seed);
        WindowSize = winsize;
        LimitSeqNo = winsize*2; // set appropriately; assumes SR here!
        RxmtInterval = delay;

        originPktNum = 0;
        retransPktNum = 0;
        deliveredPktNum = 0;
        ackPktNum = 0;
        corruptPktNum = 0;
        receivedPktNum = 0;
        currSeqNum_a = 0;

        RTTList = new ArrayList<Double>();
        CTList = new ArrayList<Double>();
        sendTime = new HashMap<Integer, Double>();
        oriSendTime = new HashMap<Integer, Double>();
    }

    // This function copies the head packet from the queue,
    // sent it and update relevant values.
    protected void sendPacket(Queue<Packet> q){
        if (q.isEmpty()){
            return;
        }
        System.out.println("Packet sent by A with seq number " + q.peek().getSeqnum() + ", payload: " + q.peek().getPayload());
        toLayer3(0, q.peek());

        if (timerFlag_a == false){
            startTimer(0, RxmtInterval);
            timerFlag_a = true;
            System.out.println("Duplicate ack received, resend the next missing packet.");
        }
        if (q.equals(unsentBuffer_a)){
            // Record the sent time to calculate RTT & communication time
            sendTime.put(q.peek().getSeqnum(), getTime());
            oriSendTime.put(q.peek().getSeqnum(), getTime());
            resentBuffer_a.add(unsentBuffer_a.poll());
            originPktNum += 1;
        }
        else{
            // Remove the record for RTT since the pkt is retransmitted.
            sendTime.remove(q.peek().getSeqnum());
            retransPktNum += 1;
        }
    }


    // This routine will be called whenever the upper layer at the sender [A]
    // has a message to send.  It is the job of your protocol to insure that
    // the data in such a message is delivered in-order, and correctly, to
    // the receiving upper layer.
    protected void aOutput(Message message)
    {
        System.out.println("Message received at A: " + message.getData());
        // Generate packet
        Packet pkt = new Packet(nextSeqNum_a, -1, message.getData().hashCode(), message.getData());
        unsentBuffer_a.add(pkt);

        // Check if the packet can be sent right away.
        if (resentBuffer_a.size() < WindowSize){
            sendPacket(unsentBuffer_a);
        }
        // Increase the next sequence number.
        nextSeqNum_a += 1;
        if (nextSeqNum_a == 2*WindowSize){
            nextSeqNum_a = 0;
        }
    }

    // This routine will be called whenever a packet sent from the B-side
    // (i.e. as a result of a toLayer3() being done by a B-side procedure)
    // arrives at the A-side.  "packet" is the (possibly corrupted) packet
    // sent from the B-side.
    protected void aInput(Packet packet)
    {
        int originAckNum = packet.getAcknum();
        int ackNum = originAckNum;
        receivedPktNum += 1;
        // Check if the packet is corrupted.
        if (packet.getSeqnum() == -1 && packet.getPayload().equals("") && ackNum >= 0 && ackNum < 2*WindowSize){
            System.out.println("Packet received at A with ack number " + originAckNum);
            // Stop timer when ack received.
            if (timerFlag_a == true){
                stopTimer(0);
                timerFlag_a = false;
            }
            // If duplicate ack received, resend next missing data.
            if (ackNum == lastAckNum_a){
                sendPacket(resentBuffer_a);
            }
            else {
                // If the new ack number is smaller than the old one, add RWS to get the number of newly acked packets.
                if (ackNum < lastAckNum_a) {
                    ackNum += 2*WindowSize;
                }
                // use reception time of the ack to calculate RTT (if the sent time is recorded in map)
                if (sendTime.containsKey(originAckNum-1)){
                    RTTList.add(getTime()-sendTime.get(originAckNum-1));
                }

                for (int i = 0; i < (ackNum-lastAckNum_a); i++){
                    resentBuffer_a.poll();
                    // use reception time of the ack to calculate CT for all packets corresponded by this ack.
                    CTList.add(getTime()-oriSendTime.get(((originAckNum-1-i)%(2*WindowSize)+(2*WindowSize))%(2*WindowSize)));
                    sendPacket(unsentBuffer_a);
                }
            }
            lastAckNum_a = originAckNum;
        }
        else{
            System.out.println("Ack packet is corrupted!");
            corruptPktNum += 1;
        }
    }

    // This routine will be called when A's timer expires (thus generating a
    // timer interrupt). You'll probably want to use this routine to control
    // the retransmission of packets. See startTimer() and stopTimer(), above,
    // for how the timer is started and stopped.
    protected void aTimerInterrupt()
    {
        System.out.println("Timeout, A resend the next missing packet.");
        if (timerFlag_a == true){
            stopTimer(0);
            timerFlag_a = false;
        }
        sendPacket(resentBuffer_a);
    }

    // This routine will be called once, before any of your other A-side
    // routines are called. It can be used to do any required
    // initialization (e.g. of member variables you add to control the state
    // of entity A).
    protected void aInit()
    {
        unsentBuffer_a = new LinkedList<Packet>();
        resentBuffer_a = new LinkedList<Packet>();
        nextSeqNum_a = 0;
        lastAckNum_a = 0;
    }


    protected void b_send_ACK(int ack){
        Packet p = new Packet(-1,ack,-1);
        System.out.println("Packet sent by B with ack number " + ack);
        toLayer3(B,p);
        ackPktNum += 1;
    }

    // This routine will be called whenever a packet sent from the B-side
    // (i.e. as a result of a toLayer3() being done by an A-side procedure)
    // arrives at the B-side.  "packet" is the (possibly corrupted) packet
    // sent from the A-side.
    //
    protected void bInput(Packet packet)
    {
        receivedPktNum += 1;
        String msg = packet.getPayload();
        int p_seq = packet.getSeqnum();
        int checksum = msg.hashCode();
        if (packet.getChecksum() != checksum || p_seq < 0 || p_seq >= 2*WindowSize || packet.getAcknum() != -1){
            System.out.println("Checksum failed, packet from A is corrupted!");
            corruptPktNum += 1;
            return;
        }
        System.out.println("Packet received at B with seq number " + p_seq + ", payload: " + msg);

        /**stop and wait model*/
        if(max_B == wanted_B){
            if (p_seq == wanted_B){
                wanted_B = (wanted_B +1)%LimitSeqNo;
                max_B = (max_B +1)%LimitSeqNo;
                toLayer5(packet.getPayload());
                deliveredPktNum++;

                b_send_ACK(wanted_B);
                return;
            }
            else {
                b_send_ACK(wanted_B);
                return;
            }
        }

        if (p_seq == wanted_B){
            buffer_B.put(wanted_B,packet);
            /**When current_B packet is received*/
            while(buffer_B.get(wanted_B) != null){
                toLayer5(buffer_B.get(wanted_B).getPayload());
                deliveredPktNum += 1;

                buffer_B.remove(wanted_B);
                max_B = (max_B+1)%LimitSeqNo;
                wanted_B = (wanted_B+1)%LimitSeqNo;
            }
            b_send_ACK(wanted_B);
        }else {
            for (int key:buffer_B.keySet()) {
                if (p_seq==key){
                    b_send_ACK(wanted_B);
                    return;
                }
            }
            if (max_B < wanted_B){
                if (p_seq <= max_B || p_seq > wanted_B){
                    buffer_B.put(p_seq,packet);
                }else {
                    b_send_ACK(wanted_B);
                }
            }
            if (max_B > wanted_B){
                if (p_seq > wanted_B && p_seq <=max_B){
                    buffer_B.put(p_seq,packet);
                }else {
                    b_send_ACK(wanted_B);
                }
            }
        }

    }

    // This routine will be called once, before any of your other B-side
    // routines are called. It can be used to do any required
    // initialization (e.g. of member variables you add to control the state
    // of entity B).
    protected void bInit()
    {
        buffer_B = new HashMap<>();
        wanted_B = 0;
        max_B = (wanted_B+WindowSize-1) % LimitSeqNo;
    }

    // Use to print final statistics
    protected void Simulation_done()
    {
        // Calculate total RTT & communication time.
        Double totalRTT = Double.valueOf(0);
        for (int i = 0; i < RTTList.size(); i++){
            totalRTT += RTTList.get(i);
        }

        Double totalCT = Double.valueOf(0);
        for (int i = 0; i < CTList.size(); i++){
            totalCT += CTList.get(i);
        }
        // TO PRINT THE STATISTICS, FILL IN THE DETAILS BY PUTTING VARIBALE NAMES. DO NOT CHANGE THE FORMAT OF PRINTED OUTPUT
        System.out.println("\n\n===============STATISTICS=======================");
        System.out.println("Number of original packets transmitted by A:" + originPktNum);
        System.out.println("Number of retransmissions by A:" + retransPktNum);
        System.out.println("Number of data packets delivered to layer 5 at B:" + deliveredPktNum);
        System.out.println("Number of ACK packets sent by B:" + ackPktNum);
        System.out.println("Number of corrupted packets:" + corruptPktNum);
        System.out.println("Ratio of lost packets:" + ((double)retransPktNum - (double)corruptPktNum)/((double)originPktNum+(double)retransPktNum+(double)ackPktNum));
        System.out.println("Ratio of corrupted packets:" + (double)corruptPktNum/((double)originPktNum+(double)retransPktNum+(double)ackPktNum-(double)retransPktNum + (double)corruptPktNum));
        System.out.println("Average RTT:" + totalRTT/RTTList.size());
        System.out.println("Average communication time:" + totalCT/CTList.size());
        System.out.println("==================================================");

        // PRINT YOUR OWN STATISTIC HERE TO CHECK THE CORRECTNESS OF YOUR PROGRAM
        System.out.println("\nEXTRA:");
        // EXAMPLE GIVEN BELOW
        //System.out.println("Example statistic you want to check e.g. number of ACK packets received by A :" + "<YourVariableHere>");
    }

}
