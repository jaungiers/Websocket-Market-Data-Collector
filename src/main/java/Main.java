import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws URISyntaxException, InterruptedException {
        System.out.println("### Starting Data Collector ###");

        String urlBitmex = "wss://www.bitmex.com/realtime";
        DataCollectorBitmex connBitmex = new DataCollectorBitmex(
                new URI(urlBitmex),
                "XBTUSD",
                10000,
                5,
                "datachunkBitmex.csv");

        System.out.println("[" + connBitmex.getDatetime() + "] Attempting to connect to Bitmex");
        connBitmex.connect();

        while(true) {
            if (connBitmex.isClosed()) {
                System.out.println("[" + connBitmex.getDatetime() + "] Connection detected as Closed. Attempting to reconnect.");
                connBitmex.connect();
            }
            TimeUnit.SECONDS.sleep(30);
        }
    }
}
