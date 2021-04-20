# Websocket Market Data Collector
This is a Java based websocket data collector for crypto L2 market data saving to HDF5 files.

Due to the high-volume of L2 data from websocket connections on crypto exchanges for highly liquid instruments,
using Python websocket scripts does not work (due to the "slow consumer" problem causing sync issues).
Hence Java has been used to provide a good balance between usability and speed.

### Currently supported exchanges:
* Bitmex

### How to run
Due to the non-trivial nature of working with HDF5 files in Java (especially the operation of appending to an
existing file to infinity) the process is split into two processes:
1) A Java data collector process which collects the L2 data and stores it as CSV chunks of specified size.
2) A Python chunk processor script which runs alongside the Java process, checks the Java generated CSV chunks
for changes and when a change is detected parses it to append to a HDF5 (.h5) datapack.
   
Recommended running: compile the Java project and create an executable JAR artifact. Run this JAR in a TMUX
window. In a seperate or split TMUX window run the Python process.