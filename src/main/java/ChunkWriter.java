import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ChunkWriter implements Runnable {

    String filepath;
    DataChunk dataChunk;
    String strDataRow;
    String bids;
    String vbids;
    String asks;
    String vasks;
    String tickDir;
    long tStart, tEnd;

    public ChunkWriter(String filepath, DataChunk dataChunk)
    {
        this.filepath = filepath;
        this.dataChunk = dataChunk;
    }

    private void clearDepthVars() {
        this.bids = "";
        this.vbids = "";
        this.asks = "";
        this.vasks = "";
        this.tickDir = "";
    }

    @Override
    public void run() {
        tStart = System.currentTimeMillis();
        try {
            FileWriter chunkFile = new FileWriter(filepath);
            clearDepthVars();
            for (int i = 0; i < dataChunk.chunkSize; i++) {
                for (int j = 0; j < dataChunk.depthSize; j++) {
                    bids += dataChunk.bids[j][i] + ",";
                    vbids += dataChunk.vbids[j][i] + ",";
                    asks += dataChunk.asks[j][i] + ",";
                    vasks += dataChunk.vasks[j][i] + ",";
                    if(j < 4) {
                        tickDir += dataChunk.tickDir[j][i] + ",";
                    }
                }
                strDataRow = dataChunk.tsLocal[0][i]+","+
                        dataChunk.tsXchng[0][i]+","+
                        bids+
                        vbids+
                        asks+
                        vasks+
                        tickDir+
                        dataChunk.last[0][i]+","+
                        dataChunk.vlast[0][i]+","+
                        dataChunk.isSell[0][i]+","+
                        dataChunk.homeNotional[0][i]+","+
                        dataChunk.foreignNotional[0][i]+","+
                        dataChunk.grossValue[0][i]+"\n";
                clearDepthVars();
                chunkFile.write(strDataRow);
            }
            chunkFile.close();

            Date date = new Date();
            SimpleDateFormat formatter = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
            tEnd = System.currentTimeMillis();
            System.out.println("Data chunk written in " + (tEnd-tStart) + "ms on " + formatter.format(date));
        } catch (IOException e) {
            System.out.println("Error writing output chunk");
            e.printStackTrace();
        }
    }
}
