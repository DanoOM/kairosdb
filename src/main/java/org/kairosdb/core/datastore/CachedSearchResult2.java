/*
 * Copyright 2016 KairosDB Authors
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package org.kairosdb.core.datastore;

import java.io.DataInputStream;
import java.io.Externalizable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.util.BufferedDataInputStream;
import org.kairosdb.util.BufferedDataOutputStream;
import org.kairosdb.util.MemoryMonitor;
import org.kairosdb.util.StringPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachedSearchResult2 implements SearchResult
{
	public static final Logger logger = LoggerFactory.getLogger(CachedSearchResult2.class);

	public static final int WRITE_BUFFER_SIZE = 500;

	public static final byte LONG_FLAG = 0x1;
	public static final byte DOUBLE_FLAG = 0x2;

	private final String m_metricName;
	private final List<FilePositionMarker> m_dataPointSets;
	private FilePositionMarker m_currentFilePositionMarker;
	private final File m_dataFile;
	private final String m_baseFileName;
	private RandomAccessFile m_randomAccessFile;
	private BufferedDataOutputStream m_dataOutputStream;

	private final File m_indexFile;
	private final AtomicInteger m_closeCounter = new AtomicInteger();
	private boolean m_readFromCache = false;
	private final KairosDataPointFactory m_dataPointFactory;
	private final StringPool m_stringPool;
	private int m_maxReadBufferSize = 8192;  //Default value in BufferedInputStream
	private boolean m_keepCacheFiles;
	private final long m_startTime;
	private final long m_timeWindow;
	private  long m_endTime;
	private String m_type;
	private Map<String,String> m_tags;
	public void setType(String type){
	    m_type  = type;
	}

	public void setTags(Map<String,String> tags){
	    m_tags = tags;
	}

	// treemap - ensure itration is in order..
	private final TreeMap<Long,CachedSearchResult2> m_buckets = new TreeMap<>();


	private static String formatTime(long time) {
	    SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMdd'-'HHmmss");
        return sdf.format(new Date(time));
	}
	private static File getIndexFile(String baseFileName, long time)
	{
	    if (time % 60_000 != 0){
	        System.out.println("?????");
	    }
	    String dt = formatTime(time);
		String indexFileName = baseFileName + dt + ".index";
		return (new File(indexFileName));
	}

	private static File getDataFile(String baseFileName, long time)
	{
        String dt = formatTime(time);
		String dataFileName = baseFileName + dt + ".data";
		return (new File(dataFileName));
	}

	private CachedSearchResult2(String metricName,
                                String baseFileName,
                                KairosDataPointFactory datatPointFactory,
                                boolean keepCacheFiles,
                                long startTime,
                                long timeWindow)
			throws FileNotFoundException
	{
	    m_baseFileName = baseFileName;
		m_metricName = metricName;
		m_indexFile = getIndexFile(baseFileName, startTime);
		m_dataPointSets = new ArrayList<FilePositionMarker>();
		m_dataFile = getDataFile(baseFileName,startTime) ;
		m_dataPointFactory = datatPointFactory;
		m_stringPool = new StringPool();
		m_keepCacheFiles = keepCacheFiles;
		m_startTime = truncate(startTime,timeWindow);
		m_timeWindow = timeWindow;
	}

	private void openCacheFile() throws FileNotFoundException
	{
		//Cache cleanup could have removed the folders
		m_dataFile.getParentFile().mkdirs();
		m_randomAccessFile = new RandomAccessFile(m_dataFile, "rw");
		m_dataOutputStream = BufferedDataOutputStream.create(m_randomAccessFile, 0L);
	}

	private void calculateMaxReadBufferSize()
	{
		//Reduce the max buffer size when we have a lot of rows to conserve memory
		if (m_dataPointSets.size() > 100000)
			m_maxReadBufferSize = 1024;
		else if (m_dataPointSets.size() > 75000)
			m_maxReadBufferSize = 1024 * 2;
		else if (m_dataPointSets.size() > 50000)
			m_maxReadBufferSize = 1024 * 4;
	}


	/**
	 Reads the index file into memory
	 */
	private void loadIndex() throws IOException, ClassNotFoundException
	{
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(m_indexFile));
		int size = in.readInt();
		for (int I = 0; I < size; I++)
		{
			//open the cache file only if there will be data point groups returned
			if (m_randomAccessFile == null)
				openCacheFile();

			FilePositionMarker marker = new FilePositionMarker();
			marker.readExternal(in);
			m_dataPointSets.add(marker);
		}


		m_readFromCache = true;
		in.close();

		calculateMaxReadBufferSize();
	}

	private void saveIndex() throws IOException
	{
		if (m_readFromCache)
			return; //No need to save if we read it from the file

		System.out.println("-- Saving Index: " + m_indexFile);
		if (!m_indexFile.getName().endsWith("00.index")){
		    System.out.println("hello?");
		}
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(m_indexFile));

		//todo: write out a type lookup table

		out.writeInt(m_dataPointSets.size());
		for (FilePositionMarker marker : m_dataPointSets)
		{
			marker.writeExternal(out);
		}

		out.flush();
		out.close();
	}

	public static long truncate(long time, long timewindow){
	    return timewindow * (time / timewindow);
	}

	public static CachedSearchResult2 createCachedSearchResult(String metricName,
                                                			String baseFileName,
                                                			KairosDataPointFactory dataPointFactory,
                                                			boolean keepCacheFiles,
                                                			long startTime,
                                                			long timeWindow)
			throws IOException
	{
		File dataFile = getDataFile(baseFileName,startTime);
		File indexFile = getIndexFile(baseFileName,startTime);

		//Just in case the file are there.
		//dataFile.delete();
		//indexFile.delete();

		CachedSearchResult2 ret = new CachedSearchResult2(metricName,
                                                         baseFileName,
                                                         dataPointFactory,
                                                         keepCacheFiles,
                                                         startTime,
                                                         timeWindow);

		return (ret);
	}

	/**

	 @param baseFileName base name of file
	 @param cacheTime The number of seconds to still open the file
	 @return The CachedSearchResult if the file exists or null if it doesn't
	 */
	public static CachedSearchResult2 openCachedSearchResult(String metricName,
                                                             String baseFileName,
                                                             int cacheTime,
                                                             KairosDataPointFactory dataPointFactory,
                                                             boolean keepCacheFiles,
                                                             long startTime,
                                                             long timeWindow,
                                                             long endTime) throws IOException
	{
	    CachedSearchResult2 ret = new CachedSearchResult2(metricName, baseFileName, dataPointFactory, keepCacheFiles, startTime,timeWindow);
		if (ret.createBuckets(startTime, endTime) == 0) {
		    // no data in cache, return.
		    ret = null;
		    return ret;
		}
		try
		{
			ret.loadIndex();
		}
		catch (ClassNotFoundException e)
		{
			logger.error("Unable to load cache file", e);
			ret = null;
		}
		return (ret);
	}

	public long getEndTime() {
	    return m_endTime;
	}

	public long getStartTime() {
	    return m_startTime;
	}

	public Map<Long,CachedSearchResult2> getBuckets(){
	    return m_buckets;
	}

	public void attachResults(CachedSearchResult2 result) {
	    //xxx long startTime = result.getStartTime();
	    long largestKey = m_buckets.lastKey()+1;
	    m_buckets.put(largestKey, result);// put the root in
	    /*if (result.getBuckets() != null) {
	        for (CachedSearchResult2 value: result.getBuckets().values()){
	            m_buckets.put(++largestKey, value);
	        }
	    }*/
	}

	// fix this..need better resutls.
	private long createBuckets(long startTime, long endTime) throws IOException {
	    boolean cacheHit = false;
	    int indexHoles = 0;
	    startTime = truncate (m_startTime, m_timeWindow);
        while(startTime < endTime) {
            File indexFile = getIndexFile(m_baseFileName, startTime);
            if (!indexFile.exists()) {
                System.out.println("Missing Data at: " + startTime);
                indexHoles++;
            }
            else {
                cacheHit = true;
                m_endTime = startTime; // this is the last time bucket found
                System.out.println("Cache hit at:" + startTime + " initial LoadTime Request:" + m_startTime + " diff: " + ((startTime - m_startTime)/1000));
                CachedSearchResult2 bucket = createCachedSearchResult(m_metricName,
                                                                      m_baseFileName ,
                                                                      m_dataPointFactory,
                                                                      m_keepCacheFiles,
                                                                      startTime,
                                                                      m_timeWindow);
                try {
                    System.out.println("loading Index for bucket: " + bucket.m_indexFile.getName());
                    bucket.loadIndex();
                }
                catch(ClassNotFoundException e) {
                    logger.error("Unable to load cache file", e);
                    return 0;
                }
                m_buckets.put(startTime, bucket);
            }
            startTime = startTime + 60_000;
        }
        if (cacheHit)
            return startTime;
        return 0;
	}

	/**
	 Call when finished adding datapoints to the cache file
	 */
	@Override
    public void endDataPoints() throws IOException
	{
	    if (m_buckets != null) {
	        for (CachedSearchResult2 bucket : m_buckets.values()) {
	            bucket.endDataPoints2();
	        }
	    }
		if (m_randomAccessFile == null)
			return;

		//flushWriteBuffer();
		m_dataOutputStream.flush();

		long curPosition = m_dataOutputStream.getPosition();
		if (m_dataPointSets.size() != 0)
			m_dataPointSets.get(m_dataPointSets.size() -1).setEndPosition(curPosition);

		calculateMaxReadBufferSize();
	}

	public void endDataPoints2() throws IOException
    {
        if (m_randomAccessFile == null)
            return;

        //flushWriteBuffer();
        m_dataOutputStream.flush();

        long curPosition = m_dataOutputStream.getPosition();
        if (m_dataPointSets.size() != 0)
            m_dataPointSets.get(m_dataPointSets.size() -1).setEndPosition(curPosition);

        calculateMaxReadBufferSize();
    }


	/**
	 Closes the underling file handle
	 */
	private void close()
	{
		try
		{
			if (m_randomAccessFile != null)
				m_randomAccessFile.close();

			if (m_keepCacheFiles)
				saveIndex();
			else
				m_dataFile.delete();
		}
		catch (IOException e)
		{
			logger.error("Failure closing cache file", e);
		}
	}

	protected void decrementClose()
	{
		if (m_closeCounter.decrementAndGet() == 0){
		    if (m_buckets != null){
		        for (CachedSearchResult2 bucket : m_buckets.values()) {
		            bucket.close();
		        }
		    }
		    close();
		}
	}


	/**
	 A new set of datapoints to write to the file.  This causes the start position
	 of the set to be saved.  All inserted datapoints after this call are
	 expected to be in ascending time order and have the same tags.
	 */
	@Override
    public void startDataPointSet(String type, Map<String, String> tags) throws IOException
	{
	    if (m_currentBucket != null) {
	        m_currentBucket.startDataPointSet2(type, tags);
	        return;
	    }
	    m_type = type;
	    m_tags = tags;
		//todo: need a lock around this, cql returns results overlapping.
		if (m_randomAccessFile == null)
			openCacheFile();

		endDataPoints();

		long curPosition = m_dataOutputStream.getPosition();
		m_currentFilePositionMarker = new FilePositionMarker(curPosition, tags, type);
		m_dataPointSets.add(m_currentFilePositionMarker);
	}

	private void startDataPointSet2(String type, Map<String, String> tags) throws IOException
    {
        m_type = type;
        m_tags = tags;
        //todo: need a lock around this, cql returns results overlapping.
        if (m_randomAccessFile == null)
            openCacheFile();

        endDataPoints2();

        long curPosition = m_dataOutputStream.getPosition();
        m_currentFilePositionMarker = new FilePositionMarker(curPosition, tags, type);
        m_dataPointSets.add(m_currentFilePositionMarker);
    }


	private CachedSearchResult2 m_currentBucket;
	private long m_currentBucketTime = 0;

	@Override
	public void addDataPoint(DataPoint datapoint) throws IOException
	{
	    long ts = datapoint.getTimestamp();
	    long timeBucket = truncate(ts, m_timeWindow);
	    if (timeBucket != m_currentBucketTime) {
	        System.out.println("new Bucket: " + formatTime(timeBucket));
	        CachedSearchResult2 bucket = m_buckets.get(timeBucket);
	        if (bucket == null)
	            bucket = createCachedSearchResult(m_metricName, m_baseFileName ,m_dataPointFactory,m_keepCacheFiles,timeBucket,m_timeWindow);
	        m_buckets.put(timeBucket, bucket);
	        m_currentBucket = bucket;
	        m_currentBucket.startDataPointSet2(m_type, m_tags);
	    }
	    if (m_currentBucket != null) {
	        m_currentBucket.addDataPt(datapoint);
	    }
	    else {
    		m_dataOutputStream.writeLong(datapoint.getTimestamp());
    		datapoint.writeValueToBuffer(m_dataOutputStream);

    		m_currentFilePositionMarker.incrementDataPointCount();
	    }
	}


    private void addDataPt(DataPoint datapoint) throws IOException
    {
            m_dataOutputStream.writeLong(datapoint.getTimestamp());
            datapoint.writeValueToBuffer(m_dataOutputStream);
            m_currentFilePositionMarker.incrementDataPointCount();
    }

	@Override
	public List<DataPointRow> getRows()
	{
		List<DataPointRow> ret = new ArrayList<DataPointRow>();
		MemoryMonitor mm = new MemoryMonitor(20);

		for (FilePositionMarker dpSet : m_dataPointSets)
		{
			ret.add(dpSet.iterator());
			m_closeCounter.incrementAndGet();
			mm.checkMemoryAndThrowException();
		}
		if (m_buckets != null) {
		    for (CachedSearchResult2 bucket: m_buckets.values()) {
		        bucket.getRows(ret);
		    }
		}
		return (ret);
	}

	public List<DataPointRow> getRows(List<DataPointRow> rows)
    {
        MemoryMonitor mm = new MemoryMonitor(20);
        for (FilePositionMarker dpSet : m_dataPointSets)
        {
            rows.add(dpSet.iterator());
            m_closeCounter.incrementAndGet();
            mm.checkMemoryAndThrowException();
        }
        return (rows);
    }


	//===========================================================================
	private class FilePositionMarker implements Iterable<DataPoint>, Externalizable
	{
		private long m_startPosition;
		private long m_endPosition;
		private Map<String, String> m_tags;
		private String m_dataType;
		private int m_dataPointCount;


		public FilePositionMarker()
		{
			m_startPosition = 0L;
			m_endPosition = 0L;
			m_tags = new HashMap<String, String>();
			m_dataType = null;
			m_dataPointCount = 0;
		}

		public FilePositionMarker(long startPosition, Map<String, String> tags,
				String dataType)
		{
			m_startPosition = startPosition;
			m_tags = tags;
			m_dataType = dataType;
		}

		public void setEndPosition(long endPosition)
		{
			m_endPosition = endPosition;
		}

		public Map<String, String> getTags()
		{
			return m_tags;
		}

		public void incrementDataPointCount()
		{
			m_dataPointCount ++;
		}

		public int getDataPointCount()
		{
			return m_dataPointCount;
		}

		@Override
		public CachedDataPointRow iterator()
		{
			return (new CachedDataPointRow(m_tags, m_startPosition, m_endPosition,
					m_dataType, m_dataPointCount));
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException
		{
			out.writeLong(m_startPosition);
			out.writeLong(m_endPosition);
			out.writeInt(m_dataPointCount);
			out.writeObject(m_dataType);
			out.writeInt(m_tags.size());
			for (String s : m_tags.keySet())
			{
				out.writeObject(s);
				out.writeObject(m_tags.get(s));
			}
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
		{
			m_startPosition = in.readLong();
			m_endPosition = in.readLong();
			m_dataPointCount = in.readInt();
			m_dataType = (String)in.readObject();
			//m_dataPointCount = (int)((m_endPosition - m_startPosition) / DATA_POINT_SIZE);

			int tagCount = in.readInt();
			for (int I = 0; I < tagCount; I++)
			{
				String key = m_stringPool.getString((String)in.readObject());
				String value = m_stringPool.getString((String)in.readObject());
				m_tags.put(key, value);
			}
		}
	}

	//===========================================================================
	private class CachedDataPointRow implements DataPointRow
	{
		private long m_currentPosition;
		private long m_endPostition;
		private DataInputStream m_readBuffer = null;
		private Map<String, String> m_tags;
		private final String m_dataType;
		private final int m_dataPointCount;
		private int m_dataPointsRead = 0;

		public CachedDataPointRow(Map<String, String> tags,
                                        				    long startPosition,
                                        				    long endPostition,
                                        				    String dataType,
                                        				    int dataPointCount)
		{
			m_currentPosition = startPosition;
			m_endPostition = endPostition;

			m_tags = tags;
			m_dataType = dataType;
			m_dataPointCount = dataPointCount;
		}

		private void allocateReadBuffer()
		{
			int rowSize = (int) (m_endPostition - m_currentPosition);
			if (m_buckets != null) {
//	xxx		    for (CachedSearchResult2 bucket: m_buckets){
			//xx        rowSize += bucket.getRowSize();
//			    }
			}
	        int bufferSize = (rowSize < m_maxReadBufferSize ? rowSize : m_maxReadBufferSize);
			m_readBuffer = new BufferedDataInputStream(m_randomAccessFile, m_currentPosition, bufferSize);
		}




		@Override
		public boolean hasNext()
		{
			return (m_dataPointsRead < m_dataPointCount);
			//return (m_readBuffer.hasRemaining() || m_currentPosition < m_endPostition);
		}

		@Override
		public DataPoint next()
		{
			DataPoint ret = null;

			try
			{
				//Lazy allocation of buffer to conserve memory when using group by's
				if (m_readBuffer == null)
					allocateReadBuffer();

				long timestamp = m_readBuffer.readLong();

				ret = m_dataPointFactory.createDataPoint(m_dataType, timestamp, m_readBuffer);

			}
			catch (IOException ioe)
			{
				logger.error("Error reading next data point.", ioe);
			}

			m_dataPointsRead ++;

			//Clean up buffer.  In cases where we are grouping not all rows are read
			//at once so this will save memory
			if (m_dataPointsRead == m_dataPointCount)
			{
				try
				{
					m_readBuffer.close();
					m_readBuffer = null;
				}
				catch (IOException e)
				{
					e.printStackTrace();
				}
			}

			return (ret);
		}

		@Override
		public void remove()
		{
			throw new UnsupportedOperationException();
		}

		@Override
		public String getName()
		{
			return (m_metricName);
		}

		@Override
		public String getDatastoreType()
		{
			return m_dataType;
		}

		@Override
		public Set<String> getTagNames()
		{
			return (m_tags.keySet());
		}

		@Override
		public String getTagValue(String tag)
		{
			return (m_tags.get(tag));
		}

		@Override
		public void close()
		{
			decrementClose();
		}

		@Override
		public int getDataPointCount()
		{
			return m_dataPointCount;
		}

		@Override
		public String toString()
		{
			return "CachedDataPointRow{" +
					"m_metricName='" + m_metricName + '\'' +
					", m_tags=" + m_tags +
					'}';
		}
	}
}
