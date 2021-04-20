public class DataChunk {

    private int idx;
    public int chunkSize;
    public int depthSize;
    public String filepath;

    public long[][] tsLocal;
    public long[][] tsXchng;
    public double[][] bids;
    public double[][] vbids;
    public double[][] asks;
    public double[][] vasks;
    public double[][] last;
    public double[][] vlast;
    public int[][] isSell;
    public int[][] tickDir;
    public double[][] homeNotional;
    public double[][] foreignNotional;
    public double[][] grossValue;

    public DataChunk(int chunkSize, int depthSize, String filepath){
        this.chunkSize = chunkSize;
        this.depthSize = depthSize;
        this.filepath = filepath;
        this.tsLocal = new long[1][chunkSize];
        this.tsXchng = new long[1][chunkSize];
        this.bids = new double[depthSize][chunkSize];
        this.vbids = new double[depthSize][chunkSize];
        this.asks = new double[depthSize][chunkSize];
        this.vasks = new double[depthSize][chunkSize];
        this.isSell = new int[1][chunkSize];
        this.last = new double[1][chunkSize];
        this.vlast = new double[1][chunkSize];
        this.tickDir = new int[4][chunkSize];
        this.homeNotional = new double[1][chunkSize];
        this.foreignNotional = new double[1][chunkSize];
        this.grossValue = new double[1][chunkSize];
    }

    public int append(long tsLocal,
                      long tsXchng,
                      double[] bids,
                      double[] vbids,
                      double[] asks,
                      double[] vasks,
                      int isSell,
                      double last,
                      double vlast,
                      int[] tickDir,
                      double homeNotional,
                      double foreignNotional,
                      double grossValue,
                      int chunkDroppedCounter) {
        this.tsLocal[0][idx] = tsLocal;
        this.tsXchng[0][idx] = tsXchng;
        for (int i = 0; i < depthSize; i++) {
            this.bids[i][idx] = bids[i];
            this.vbids[i][idx] = vbids[i];
            this.asks[i][idx] = asks[i];
            this.vasks[i][idx] = vasks[i];
            if(i < 4) {
                this.tickDir[i][idx] = tickDir[i];
            }
        }
        this.isSell[0][idx] = isSell;
        this.last[0][idx] = last;
        this.vlast[0][idx] = vlast;
        this.homeNotional[0][idx] = homeNotional;
        this.foreignNotional[0][idx] = foreignNotional;
        this.grossValue[0][idx] = grossValue;
        idx++;

        if(idx >= chunkSize) {
            ChunkWriter chunkWriter = new ChunkWriter(filepath, cloneDataChunk());
            Thread threadWriter = new Thread(chunkWriter);
            threadWriter.start();
            idx = 0;
            chunkDroppedCounter = 0;
        }
        return chunkDroppedCounter;
    }

    public DataChunk cloneDataChunk() {
        DataChunk writeableChunk = new DataChunk(chunkSize, depthSize, filepath);
        writeableChunk.tsLocal = tsLocal;
        writeableChunk.tsXchng = tsXchng;
        writeableChunk.bids = bids;
        writeableChunk.vbids = vbids;
        writeableChunk.asks = asks;
        writeableChunk.vasks = vasks;
        writeableChunk.last = last;
        writeableChunk.vlast = vlast;
        writeableChunk.isSell = isSell;
        writeableChunk.tickDir = tickDir;
        writeableChunk.homeNotional = homeNotional;
        writeableChunk.foreignNotional = foreignNotional;
        writeableChunk.grossValue = grossValue;
        return writeableChunk;
    }
}
