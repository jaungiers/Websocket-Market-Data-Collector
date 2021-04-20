import java.net.URI;
import java.net.URISyntaxException;

public class Main {
    public static void main(String[] args) throws URISyntaxException {
        System.out.println("### Starting Data Collector ###");

        String urlBitmex = "wss://www.bitmex.com/realtime";
        DataCollectorBitmex connBitmex = new DataCollectorBitmex(
                new URI(urlBitmex),
                "XBTUSD",
                10000,
                5,
                "datachunkBitmex.csv");
        System.out.println("Attempting to connect to Bitmex");
        connBitmex.connect();
    }
}
