package org.kairosdb.datastore.cassandra;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.util.BufferedDataInputStream;
import org.kairosdb.util.BufferedDataOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BucketCache {
        public static final Logger logger = LoggerFactory.getLogger(BucketCache.class);
        private long m_timeWindow = 60_000;
        BufferedDataOutputStream m_dataOutputStream;
        private RandomAccessFile m_randomAccessFile;
        private File m_currentCacheFile;
        private long currentTimeWindow = 0;
        private KairosDataPointFactory m_dataPointFactory;
        int dataPointsInCache = 0;
        
        /*public static BucketCache openCacheFile(File f) {
            BucketCache c = new BucketCache();
            c.m_currentCacheFile = f;
            
        }*/
        
        public BucketCache(KairosDataPointFactory dataPointFactory){
            m_dataPointFactory = dataPointFactory;
        }
        
        public BucketCache(KairosDataPointFactory dataPointFactory, File f){
            m_dataPointFactory = dataPointFactory;
            m_currentCacheFile = f;
        }
        
        public String getName(){
            return m_currentCacheFile.getName();
        }
        
        public void writeToCache(String cacheFile, DataPoint datapoint) throws IOException, FileNotFoundException{
            cacheFile = "/tmp/cache/" + cacheFile;
            long ts = truncate(datapoint.getTimestamp(), m_timeWindow);
            if (m_dataOutputStream == null) {
                m_currentCacheFile = new File(cacheFile);
                openCacheFile(m_currentCacheFile);
                currentTimeWindow = ts;
            }
            else if (ts != currentTimeWindow) {
                currentTimeWindow = ts;                
                closeCacheFile();
                openCacheFile(m_currentCacheFile = new File(cacheFile));                
            }
            
            dataPointsInCache++;
            m_dataOutputStream.writeLong(datapoint.getTimestamp());
            datapoint.writeValueToBuffer(m_dataOutputStream);
        }
        
        public void closeCacheFile() throws IOException {
            System.out.println("closing cache with dataPoints: " + dataPointsInCache + " " + m_currentCacheFile.getName());
            if (m_dataOutputStream != null) {
                m_dataOutputStream.flush();
            }
            else {
                System.out.println("closing cache NO OUTPUTSTREAM!: " + dataPointsInCache);
            }
            
        }
        
        private void openCacheFile(File cacheFile) throws FileNotFoundException
        {
            //Cache cleanup could have removed the folders
            cacheFile.getParentFile().mkdirs();
            m_randomAccessFile = new RandomAccessFile(cacheFile, "rw");
            m_dataOutputStream = BufferedDataOutputStream.create(m_randomAccessFile, 0L);
        }
        
        public static long truncate(long time, long timewindow){
            return timewindow * (time / timewindow);
        }
        
        BufferedDataInputStream m_readBuffer;           
        public DataPoint next(String dataType) {
            try {
                if (m_readBuffer == null) {
                    m_randomAccessFile = new RandomAccessFile(m_currentCacheFile, "r");
                    m_readBuffer = new BufferedDataInputStream(m_randomAccessFile, 0, 8192);                    
                }
                DataPoint ret = null;
                long timestamp = 0;
                try {                    
                    timestamp = m_readBuffer.readLong();
                    dataPointsInCache++;
                }catch(Exception e) {
                    System.out.println("no more datapoints in cache AT: " + dataPointsInCache + " " + m_currentCacheFile.getName());
                    // no more data.. (hack for now since we don't know here the eof is..
                    return null;
                }
                ret = m_dataPointFactory.createDataPoint(dataType, timestamp, m_readBuffer);
                return ret;
            } catch(IOException e) {
                logger.error("Error reading next data point", e);
            }          
            return null;
        }
}
