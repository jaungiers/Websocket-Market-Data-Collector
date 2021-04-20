import java.net.URI;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.drafts.Draft;
import org.java_websocket.handshake.ServerHandshake;
import org.json.JSONArray;
import org.json.JSONObject;

public class DataCollectorBitmex extends WebSocketClient {

    private String instrument;
    private int depthSize;
    private long tsXchngLast = -1;
    private long totalDroppedCounter = 0;
    private int chunkDroppedCounter = 0;

    long tsLocal = -1;
    long tsXchng = -1;

    int isSell = 0;
    double last = 0;
    double vlast = 0;
    double homeNotional = 0;
    double foreignNotional = 0;
    double grossValue = 0;
    int[] tickDir = {0,0,0,0};

    private double[] bids;
    private double[] vbids;
    private double[] asks;
    private double[] vasks;

    private DataChunk dataChunk;

    public DataCollectorBitmex(URI serverUri, Draft draft) {
        super(serverUri, draft);
    }

    public DataCollectorBitmex(URI serverUri, Map<String, String> httpHeaders) {
        super(serverUri, httpHeaders);
    }

    public DataCollectorBitmex(URI serverURI, String instrument, int chunkSize, int depthSize, String filepath) {
        super(serverURI);
        this.instrument = instrument;
        this.depthSize = depthSize;
        this.dataChunk = new DataChunk(chunkSize, depthSize, filepath);

        this.bids = new double[depthSize];
        this.vbids = new double[depthSize];
        this.asks = new double[depthSize];
        this.vasks = new double[depthSize];
    }

    @Override
    public void onOpen(ServerHandshake handshakedata) {
        System.out.println("Connected to websocket");
        String subscriptionReq = "{\"op\": \"subscribe\", \"args\": [\"orderBook10:" + instrument + "\", \"trade:" + instrument + "\"]}";
        send(subscriptionReq);
        System.out.println("Attempting to subscribe to " + instrument);
        // if you plan to refuse connection based on ip or httpfields overload: onWebsocketHandshakeReceivedAsClient
    }

    @Override
    public void onMessage(String dataStr) {
        JSONObject dataObj = new JSONObject(dataStr);
        if(dataObj.has("table")) {
            JSONObject dataObjArr = dataObj.getJSONArray("data").getJSONObject(0);
            if(!dataObjArr.getString("symbol").equals(instrument)) {
                System.out.println("Socket symbol does not match " + instrument);
                return;
            }

            tsLocal = System.currentTimeMillis();
            tsXchng = parseTimestamp(dataObjArr.getString("timestamp"));
            if(tsXchng < tsXchngLast && tsXchngLast > 0){
                totalDroppedCounter++;
                chunkDroppedCounter++;
                System.out.println("Dropping data because timestamp behind latest. Current drop counts Chunk: " + chunkDroppedCounter + " Total: " + totalDroppedCounter);
                return;
            }

            tsXchngLast = tsXchng;
            isSell = 0;
            last = 0;
            vlast = 0;
            homeNotional = 0;
            foreignNotional = 0;
            grossValue = 0;
            Arrays.fill(tickDir, 0);

            if(dataObj.getString("table").equals("trade")) {
                if(dataObjArr.getInt("size") == 0) {
                    System.out.println("Dropping trade with size 0");
                    return;
                }
                isSell = dataObjArr.getString("side").equals("Sell") ? -1 : 1;
                last = dataObjArr.getDouble("price");
                vlast = dataObjArr.getDouble("size");
                homeNotional = dataObjArr.getDouble("homeNotional");
                foreignNotional = dataObjArr.getDouble("foreignNotional");
                grossValue = dataObjArr.getDouble("grossValue");
                switch(dataObjArr.getString("tickDirection")) {
                    case "PlusTick":
                        tickDir[0] = 1;
                        break;
                    case "MinusTick":
                        tickDir[1] = 1;
                        break;
                    case "ZeroPlusTick":
                        tickDir[2] = 1;
                        break;
                    case "ZeroMinusTick":
                        tickDir[3] = 1;
                        break;
                }
            } else if(dataObj.getString("table").equals("orderBook10")) {
                JSONArray bidsArr = dataObjArr.getJSONArray("bids");
                JSONArray asksArr = dataObjArr.getJSONArray("asks");
                for (int i = 0; i < depthSize; i++) {
                    bids[i] = bidsArr.getJSONArray(i).getDouble(0);
                    vbids[i] = bidsArr.getJSONArray(i).getDouble(1);
                    asks[i] = asksArr.getJSONArray(i).getDouble(0);
                    vasks[i] = asksArr.getJSONArray(i).getDouble(1);
                }
            }
            chunkDroppedCounter = dataChunk.append(tsLocal, tsXchng, bids, vbids, asks, vasks, isSell, last, vlast, tickDir, homeNotional, foreignNotional, grossValue, chunkDroppedCounter);
        } else {
            System.out.println("Received: " + dataStr);
        }
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
        System.out.println("Connection closed by " + (remote ? "remote" : "local") + " Code: " + code + " Reason: " + reason);
    }

    @Override
    public void onError(Exception e) {
        System.out.println("Error thrown");
        e.printStackTrace();
    }

    public Long parseTimestamp(String datetime) {
        Timestamp timestamp = null;
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            Date parsedDate = dateFormat.parse(datetime);
            timestamp = new Timestamp(parsedDate.getTime());
        } catch(Exception e) {
            System.out.println("Error parsing remote timestamp");
            e.printStackTrace();
        }
        return timestamp.getTime();
    }
}