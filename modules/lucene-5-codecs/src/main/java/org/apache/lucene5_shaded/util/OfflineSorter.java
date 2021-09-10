/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene5_shaded.util;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * On-disk sorting of byte arrays. Each byte array (entry) is a composed of the following
 * fields:
 * <ul>
 *   <li>(two bytes) length of the following byte array,
 *   <li>exactly the above count of bytes for the sequence to be sorted.
 * </ul>
 * 
 * @see #sort(Path, Path)
 * @lucene.experimental
 * @lucene.internal
 */
public final class OfflineSorter {

  private static Path DEFAULT_TEMP_DIR;

  /** Convenience constant for megabytes */
  public final static long MB = 1024 * 1024;
  /** Convenience constant for gigabytes */
  public final static long GB = MB * 1024;
  
  /**
   * Minimum recommended buffer size for sorting.
   */
  public final static long MIN_BUFFER_SIZE_MB = 32;

  /**
   * Absolute minimum required buffer size for sorting.
   */
  public static final long ABSOLUTE_MIN_SORT_BUFFER_SIZE = MB / 2;
  private static final String MIN_BUFFER_SIZE_MSG = "At least 0.5MB RAM buffer is needed";

  /**
   * Maximum number of temporary files before doing an intermediate merge.
   */
  public final static int MAX_TEMPFILES = 128;

  /** 
   * A bit more descriptive unit for constructors.
   * 
   * @see #automatic()
   * @see #megabytes(long)
   */
  public static final class BufferSize {
    final int bytes;
  
    private BufferSize(long bytes) {
      if (bytes > Integer.MAX_VALUE) {
        throw new IllegalArgumentException("Buffer too large for Java ("
            + (Integer.MAX_VALUE / MB) + "mb max): " + bytes);
      }
      
      if (bytes < ABSOLUTE_MIN_SORT_BUFFER_SIZE) {
        throw new IllegalArgumentException(MIN_BUFFER_SIZE_MSG + ": " + bytes);
      }
  
      this.bytes = (int) bytes;
    }
    
    /**
     * Creates a {@link BufferSize} in MB. The given 
     * values must be &gt; 0 and &lt; 2048.
     */
    public static BufferSize megabytes(long mb) {
      return new BufferSize(mb * MB);
    }
  
    /** 
     * Approximately half of the currently available free heap, but no less
     * than {@link #ABSOLUTE_MIN_SORT_BUFFER_SIZE}. However if current heap allocation 
     * is insufficient or if there is a large portion of unallocated heap-space available 
     * for sorting consult with max allowed heap size. 
     */
    public static BufferSize automatic() {
      Runtime rt = Runtime.getRuntime();
      
      // take sizes in "conservative" order
      final long max = rt.maxMemory(); // max allocated
      final long total = rt.totalMemory(); // currently allocated
      final long free = rt.freeMemory(); // unused portion of currently allocated
      final long totalAvailableBytes = max - total + free;
      
      // by free mem (attempting to not grow the heap for this)
      long sortBufferByteSize = free/2;
      final long minBufferSizeBytes = MIN_BUFFER_SIZE_MB*MB;
      if (sortBufferByteSize <  minBufferSizeBytes
          || totalAvailableBytes > 10 * minBufferSizeBytes) { // lets see if we need/should to grow the heap 
        if (totalAvailableBytes/2 > minBufferSizeBytes){ // there is enough mem for a reasonable buffer
          sortBufferByteSize = totalAvailableBytes/2; // grow the heap
        } else {
          //heap seems smallish lets be conservative fall back to the free/2 
          sortBufferByteSize = Math.max(ABSOLUTE_MIN_SORT_BUFFER_SIZE, sortBufferByteSize);
        }
      }
      return new BufferSize(Math.min((long)Integer.MAX_VALUE, sortBufferByteSize));
    }
  }
  
  /**
   * Sort info (debugging mostly).
   */
  public class SortInfo {
    /** number of temporary files created when merging partitions */
    public int tempMergeFiles;
    /** number of partition merges */
    public int mergeRounds;
    /** number of lines of data read */
    public int lines;
    /** time spent merging sorted partitions (in milliseconds) */
    public long mergeTime;
    /** time spent sorting data (in milliseconds) */
    public long sortTime;
    /** total time spent (in milliseconds) */
    public long totalTime;
    /** time spent in i/o read (in milliseconds) */
    public long readTime;
    /** read buffer size (in bytes) */
    public final long bufferSize = ramBufferSize.bytes;
    
    /** create a new SortInfo (with empty statistics) for debugging */
    public SortInfo() {}
    
    @Override
    public String toString() {
      return String.format(Locale.ROOT,
          "time=%.2f sec. total (%.2f reading, %.2f sorting, %.2f merging), lines=%d, temp files=%d, merges=%d, soft ram limit=%.2f MB",
          totalTime / 1000.0d, readTime / 1000.0d, sortTime / 1000.0d, mergeTime / 1000.0d,
          lines, tempMergeFiles, mergeRounds,
          (double) bufferSize / MB);
    }
  }

  private final BufferSize ramBufferSize;
  private final Path tempDirectory;
  
  private final Counter bufferBytesUsed = Counter.newCounter();
  private final BytesRefArray buffer = new BytesRefArray(bufferBytesUsed);
  private SortInfo sortInfo;
  private int maxTempFiles;
  private final Comparator<BytesRef> comparator;
  
  /** Default comparator: sorts in binary (codepoint) order */
  public static final Comparator<BytesRef> DEFAULT_COMPARATOR = BytesRef.getUTF8SortedAsUnicodeComparator();

  /**
   * Defaults constructor.
   * 
   * @see #getDefaultTempDir()
   * @see BufferSize#automatic()
   */
  public OfflineSorter() throws IOException {
    this(DEFAULT_COMPARATOR, BufferSize.automatic(), getDefaultTempDir(), MAX_TEMPFILES);
  }
  
  /**
   * Defaults constructor with a custom comparator.
   * 
   * @see #getDefaultTempDir()
   * @see BufferSize#automatic()
   */
  public OfflineSorter(Comparator<BytesRef> comparator) throws IOException {
    this(comparator, BufferSize.automatic(), getDefaultTempDir(), MAX_TEMPFILES);
  }

  /**
   * All-details constructor.
   */
  public OfflineSorter(Comparator<BytesRef> comparator, BufferSize ramBufferSize, Path tempDirectory, int maxTempfiles) {
    if (ramBufferSize.bytes < ABSOLUTE_MIN_SORT_BUFFER_SIZE) {
      throw new IllegalArgumentException(MIN_BUFFER_SIZE_MSG + ": " + ramBufferSize.bytes);
    }
    
    if (maxTempfiles < 2) {
      throw new IllegalArgumentException("maxTempFiles must be >= 2");
    }

    this.ramBufferSize = ramBufferSize;
    this.tempDirectory = tempDirectory;
    this.maxTempFiles = maxTempfiles;
    this.comparator = comparator;
  }

  /** 
   * Sort input to output, explicit hint for the buffer size. The amount of allocated
   * memory may deviate from the hint (may be smaller or larger).  
   */
  public SortInfo sort(Path input, Path output) throws IOException {
    sortInfo = new SortInfo();
    sortInfo.totalTime = System.currentTimeMillis();

    // NOTE: don't remove output here: its existence (often created by the caller
    // up above using Files.createTempFile) prevents another concurrent caller
    // of this API (from a different thread) from incorrectly re-using this file name

    ArrayList<Path> merges = new ArrayList<>();
    boolean success3 = false;
    try {
      ByteSequencesReader is = new ByteSequencesReader(input);
      boolean success = false;
      try {
        int lines = 0;
        while ((lines = readPartition(is)) > 0) {
          merges.add(sortPartition(lines));
          sortInfo.tempMergeFiles++;
          sortInfo.lines += lines;

          // Handle intermediate merges.
          if (merges.size() == maxTempFiles) {
            Path intermediate = Files.createTempFile(tempDirectory, "sort", "intermediate");
            boolean success2 = false;
            try {
              mergePartitions(merges, intermediate);
              success2 = true;
            } finally {
              if (success2) {
                IOUtils.deleteFilesIfExist(merges);
              } else {
                IOUtils.deleteFilesIgnoringExceptions(merges);
              }
              merges.clear();
              merges.add(intermediate);
            }
            sortInfo.tempMergeFiles++;
          }
        }
        success = true;
      } finally {
        if (success) {
          IOUtils.close(is);
        } else {
          IOUtils.closeWhileHandlingException(is);
        }
      }

      // One partition, try to rename or copy if unsuccessful.
      if (merges.size() == 1) {     
        Files.move(merges.get(0), output, StandardCopyOption.REPLACE_EXISTING);
      } else { 
        // otherwise merge the partitions with a priority queue.
        mergePartitions(merges, output);
      }
      success3 = true;
    } finally {
      if (success3) {
        IOUtils.deleteFilesIfExist(merges);
      } else {
        IOUtils.deleteFilesIgnoringExceptions(merges);
        IOUtils.deleteFilesIgnoringExceptions(output);
      }
    }

    sortInfo.totalTime = (System.currentTimeMillis() - sortInfo.totalTime); 
    return sortInfo;
  }

  /** Used by test framework */
  static void setDefaultTempDir(Path tempDir) {
    DEFAULT_TEMP_DIR = tempDir;
  }

  /**
   * Returns the default temporary directory. By default, java.io.tmpdir. If not accessible
   * or not available, an IOException is thrown
   */
  public synchronized static Path getDefaultTempDir() throws IOException {
    if (DEFAULT_TEMP_DIR == null) {
      // Lazy init
      String tempDirPath = System.getProperty("java.io.tmpdir");
      if (tempDirPath == null)  {
        throw new IOException("Java has no temporary folder property (java.io.tmpdir)?");
      }
      Path tempDirectory = Paths.get(tempDirPath);
      if (Files.isWritable(tempDirectory) == false) {
        throw new IOException("Java's temporary folder not present or writeable?: " 
                              + tempDirectory.toAbsolutePath());
      }
      DEFAULT_TEMP_DIR = tempDirectory;
    }

    return DEFAULT_TEMP_DIR;
  }

  /** Sort a single partition in-memory. */
  protected Path sortPartition(int len) throws IOException {
    BytesRefArray data = this.buffer;
    Path tempFile = Files.createTempFile(tempDirectory, "sort", "partition");

    long start = System.currentTimeMillis();
    sortInfo.sortTime += (System.currentTimeMillis() - start);
    
    final ByteSequencesWriter out = new ByteSequencesWriter(tempFile);
    BytesRef spare;
    try {
      BytesRefIterator iter = buffer.iterator(comparator);
      while((spare = iter.next()) != null) {
        assert spare.length <= Short.MAX_VALUE;
        out.write(spare);
      }
      
      out.close();

      // Clean up the buffer for the next partition.
      data.clear();
      return tempFile;
    } finally {
      IOUtils.close(out);
    }
  }

  /** Merge a list of sorted temporary files (partitions) into an output file */
  void mergePartitions(List<Path> merges, Path outputFile) throws IOException {
    long start = System.currentTimeMillis();

    ByteSequencesWriter out = new ByteSequencesWriter(outputFile);

    PriorityQueue<FileAndTop> queue = new PriorityQueue<FileAndTop>(merges.size()) {
      @Override
      protected boolean lessThan(FileAndTop a, FileAndTop b) {
        return comparator.compare(a.current.get(), b.current.get()) < 0;
      }
    };

    ByteSequencesReader [] streams = new ByteSequencesReader [merges.size()];
    try {
      // Open streams and read the top for each file
      for (int i = 0; i < merges.size(); i++) {
        streams[i] = new ByteSequencesReader(merges.get(i));
        byte line[] = streams[i].read();
        if (line != null) {
          queue.insertWithOverflow(new FileAndTop(i, line));
        }
      }
  
      // Unix utility sort() uses ordered array of files to pick the next line from, updating
      // it as it reads new lines. The PQ used here is a more elegant solution and has 
      // a nicer theoretical complexity bound :) The entire sorting process is I/O bound anyway
      // so it shouldn't make much of a difference (didn't check).
      FileAndTop top;
      while ((top = queue.top()) != null) {
        out.write(top.current.bytes(), 0, top.current.length());
        if (!streams[top.fd].read(top.current)) {
          queue.pop();
        } else {
          queue.updateTop();
        }
      }
  
      sortInfo.mergeTime += System.currentTimeMillis() - start;
      sortInfo.mergeRounds++;
    } finally {
      // The logic below is: if an exception occurs in closing out, it has a priority over exceptions
      // happening in closing streams.
      try {
        IOUtils.close(streams);
      } finally {
        IOUtils.close(out);
      }
    }
  }

  /** Read in a single partition of data */
  int readPartition(ByteSequencesReader reader) throws IOException {
    long start = System.currentTimeMillis();
    final BytesRef scratch = new BytesRef();
    while ((scratch.bytes = reader.read()) != null) {
      scratch.length = scratch.bytes.length; 
      buffer.append(scratch);
      // Account for the created objects.
      // (buffer slots do not account to buffer size.) 
      if (ramBufferSize.bytes < bufferBytesUsed.get()) {
        break;
      }
    }
    sortInfo.readTime += (System.currentTimeMillis() - start);
    return buffer.size();
  }

  static class FileAndTop {
    final int fd;
    final BytesRefBuilder current;

    FileAndTop(int fd, byte[] firstLine) {
      this.fd = fd;
      this.current = new BytesRefBuilder();
      this.current.copyBytes(firstLine, 0, firstLine.length);
    }
  }

  /**
   * Utility class to emit length-prefixed byte[] entries to an output stream for sorting.
   * Complementary to {@link ByteSequencesReader}.
   */
  public static class ByteSequencesWriter implements Closeable {
    private final DataOutput os;

    /** Constructs a ByteSequencesWriter to the provided Path */
    public ByteSequencesWriter(Path path) throws IOException {
      this(new DataOutputStream(
          new BufferedOutputStream(
              Files.newOutputStream(path))));
    }

    /** Constructs a ByteSequencesWriter to the provided DataOutput */
    public ByteSequencesWriter(DataOutput os) {
      this.os = os;
    }

    /**
     * Writes a BytesRef.
     * @see #write(byte[], int, int)
     */
    public void write(BytesRef ref) throws IOException {
      assert ref != null;
      write(ref.bytes, ref.offset, ref.length);
    }

    /**
     * Writes a byte array.
     * @see #write(byte[], int, int)
     */
    public void write(byte [] bytes) throws IOException {
      write(bytes, 0, bytes.length);
    }

    /**
     * Writes a byte array.
     * <p>
     * The length is written as a <code>short</code>, followed
     * by the bytes.
     */
    public void write(byte [] bytes, int off, int len) throws IOException {
      assert bytes != null;
      assert off >= 0 && off + len <= bytes.length;
      assert len >= 0;
      if (len > Short.MAX_VALUE) {
        throw new IllegalArgumentException("len must be <= " + Short.MAX_VALUE + "; got " + len);
      }
      os.writeShort(len);
      os.write(bytes, off, len);
    }
    
    /**
     * Closes the provided {@link DataOutput} if it is {@link Closeable}.
     */
    @Override
    public void close() throws IOException {
      if (os instanceof Closeable) {
        ((Closeable) os).close();
      }
    }    
  }

  /**
   * Utility class to read length-prefixed byte[] entries from an input.
   * Complementary to {@link ByteSequencesWriter}.
   */
  public static class ByteSequencesReader implements Closeable {
    private final DataInput is;

    /** Constructs a ByteSequencesReader from the provided Path */
    public ByteSequencesReader(Path path) throws IOException {
      this(new DataInputStream(
          new BufferedInputStream(
              Files.newInputStream(path))));
    }

    /** Constructs a ByteSequencesReader from the provided DataInput */
    public ByteSequencesReader(DataInput is) {
      this.is = is;
    }

    /**
     * Reads the next entry into the provided {@link BytesRef}. The internal
     * storage is resized if needed.
     * 
     * @return Returns <code>false</code> if EOF occurred when trying to read
     * the header of the next sequence. Returns <code>true</code> otherwise.
     * @throws EOFException if the file ends before the full sequence is read.
     */
    public boolean read(BytesRefBuilder ref) throws IOException {
      short length;
      try {
        length = is.readShort();
      } catch (EOFException e) {
        return false;
      }

      ref.grow(length);
      ref.setLength(length);
      is.readFully(ref.bytes(), 0, length);
      return true;
    }

    /**
     * Reads the next entry and returns it if successful.
     * 
     * @see #read(BytesRefBuilder)
     * 
     * @return Returns <code>null</code> if EOF occurred before the next entry
     * could be read.
     * @throws EOFException if the file ends before the full sequence is read.
     */
    public byte[] read() throws IOException {
      short length;
      try {
        length = is.readShort();
      } catch (EOFException e) {
        return null;
      }

      assert length >= 0 : "Sanity: sequence length < 0: " + length;
      byte [] result = new byte [length];
      is.readFully(result);
      return result;
    }

    /**
     * Closes the provided {@link DataInput} if it is {@link Closeable}.
     */
    @Override
    public void close() throws IOException {
      if (is instanceof Closeable) {
        ((Closeable) is).close();
      }
    }
  }

  /** Returns the comparator in use to sort entries */
  public Comparator<BytesRef> getComparator() {
    return comparator;
  }  
}
