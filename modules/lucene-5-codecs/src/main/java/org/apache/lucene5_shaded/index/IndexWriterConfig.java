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
package org.apache.lucene5_shaded.index;


import java.io.PrintStream;

import org.apache.lucene5_shaded.analysis.Analyzer;
import org.apache.lucene5_shaded.codecs.Codec;
import org.apache.lucene5_shaded.index.IndexWriter.IndexReaderWarmer;
import org.apache.lucene5_shaded.search.similarities.Similarity;
import org.apache.lucene5_shaded.store.SleepingLockWrapper;
import org.apache.lucene5_shaded.util.InfoStream;
import org.apache.lucene5_shaded.util.PrintStreamInfoStream;
import org.apache.lucene5_shaded.util.SetOnce;
import org.apache.lucene5_shaded.util.SetOnce.AlreadySetException;

/**
 * Holds all the configuration that is used to create an {@link IndexWriter}.
 * Once {@link IndexWriter} has been created with this object, changes to this
 * object will not affect the {@link IndexWriter} instance. For that, use
 * {@link LiveIndexWriterConfig} that is returned from {@link IndexWriter#getConfig()}.
 * 
 * <p>
 * All setter methods return {@link IndexWriterConfig} to allow chaining
 * settings conveniently, for example:
 * 
 * <pre class="prettyprint">
 * IndexWriterConfig conf = new IndexWriterConfig(analyzer);
 * conf.setter1().setter2();
 * </pre>
 * 
 * @see IndexWriter#getConfig()
 * 
 * @since 3.1
 */
public final class IndexWriterConfig extends LiveIndexWriterConfig {

  /**
   * Specifies the open mode for {@link IndexWriter}.
   */
  public static enum OpenMode {
    /** 
     * Creates a new index or overwrites an existing one. 
     */
    CREATE,
    
    /** 
     * Opens an existing index. 
     */
    APPEND,
    
    /** 
     * Creates a new index if one does not exist,
     * otherwise it opens the index and documents will be appended. 
     */
    CREATE_OR_APPEND 
  }

  /** Denotes a flush trigger is disabled. */
  public final static int DISABLE_AUTO_FLUSH = -1;

  /** Disabled by default (because IndexWriter flushes by RAM usage by default). */
  public final static int DEFAULT_MAX_BUFFERED_DELETE_TERMS = DISABLE_AUTO_FLUSH;

  /** Disabled by default (because IndexWriter flushes by RAM usage by default). */
  public final static int DEFAULT_MAX_BUFFERED_DOCS = DISABLE_AUTO_FLUSH;

  /**
   * Default value is 16 MB (which means flush when buffered docs consume
   * approximately 16 MB RAM).
   */
  public final static double DEFAULT_RAM_BUFFER_SIZE_MB = 16.0;

  /**
   * Default value for the write lock timeout (0 ms: no sleeping).
   * @deprecated Use {@link SleepingLockWrapper} if you want sleeping.
   */
  @Deprecated
  public static final long WRITE_LOCK_TIMEOUT = 0;

  /** Default setting for {@link #setReaderPooling}. */
  public final static boolean DEFAULT_READER_POOLING = false;

  /** Default value is 1945. Change using {@link #setRAMPerThreadHardLimitMB(int)} */
  public static final int DEFAULT_RAM_PER_THREAD_HARD_LIMIT_MB = 1945;
  
  /** Default value for compound file system for newly written segments
   *  (set to <code>true</code>). For batch indexing with very large 
   *  ram buffers use <code>false</code> */
  public final static boolean DEFAULT_USE_COMPOUND_FILE_SYSTEM = true;
  
  /** Default value for whether calls to {@link IndexWriter#close()} include a commit. */
  public final static boolean DEFAULT_COMMIT_ON_CLOSE = true;
  
  // indicates whether this config instance is already attached to a writer.
  // not final so that it can be cloned properly.
  private SetOnce<IndexWriter> writer = new SetOnce<>();
  
  /**
   * Sets the {@link IndexWriter} this config is attached to.
   * 
   * @throws AlreadySetException
   *           if this config is already attached to a writer.
   */
  IndexWriterConfig setIndexWriter(IndexWriter writer) {
    if (this.writer.get() != null) {
      throw new IllegalStateException("do not share IndexWriterConfig instances across IndexWriters");
    }
    this.writer.set(writer);
    return this;
  }
  
  /**
   * Creates a new config that with the default {@link
   * Analyzer}. By default, {@link TieredMergePolicy} is used
   * for merging;
   * Note that {@link TieredMergePolicy} is free to select
   * non-contiguous merges, which means docIDs may not
   * remain monotonic over time.  If this is a problem you
   * should switch to {@link LogByteSizeMergePolicy} or
   * {@link LogDocMergePolicy}.
   */
  public IndexWriterConfig(Analyzer analyzer) {
    super(analyzer);
  }

  /** Specifies {@link OpenMode} of the index.
   * 
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setOpenMode(OpenMode openMode) {
    if (openMode == null) {
      throw new IllegalArgumentException("openMode must not be null");
    }
    this.openMode = openMode;
    return this;
  }

  @Override
  public OpenMode getOpenMode() {
    return openMode;
  }

  /**
   * Expert: allows an optional {@link IndexDeletionPolicy} implementation to be
   * specified. You can use this to control when prior commits are deleted from
   * the index. The default policy is {@link KeepOnlyLastCommitDeletionPolicy}
   * which removes all prior commits as soon as a new commit is done (this
   * matches behavior before 2.2). Creating your own policy can allow you to
   * explicitly keep previous "point in time" commits alive in the index for
   * some time, to allow readers to refresh to the new commit without having the
   * old commit deleted out from under them. This is necessary on filesystems
   * like NFS that do not support "delete on last close" semantics, which
   * Lucene's "point in time" search normally relies on.
   * <p>
   * <b>NOTE:</b> the deletion policy cannot be null.
   *
   * <p>Only takes effect when IndexWriter is first created. 
   */
  public IndexWriterConfig setIndexDeletionPolicy(IndexDeletionPolicy delPolicy) {
    if (delPolicy == null) {
      throw new IllegalArgumentException("indexDeletionPolicy must not be null");
    }
    this.delPolicy = delPolicy;
    return this;
  }

  @Override
  public IndexDeletionPolicy getIndexDeletionPolicy() {
    return delPolicy;
  }

  /**
   * Expert: allows to open a certain commit point. The default is null which
   * opens the latest commit point.  This can also be used to open {@link IndexWriter}
   * from a near-real-time reader, if you pass the reader's
   * {@link DirectoryReader#getIndexCommit}.
   *
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setIndexCommit(IndexCommit commit) {
    this.commit = commit;
    return this;
  }

  @Override
  public IndexCommit getIndexCommit() {
    return commit;
  }

  /**
   * Expert: set the {@link Similarity} implementation used by this IndexWriter.
   * <p>
   * <b>NOTE:</b> the similarity cannot be null.
   *
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setSimilarity(Similarity similarity) {
    if (similarity == null) {
      throw new IllegalArgumentException("similarity must not be null");
    }
    this.similarity = similarity;
    return this;
  }

  @Override
  public Similarity getSimilarity() {
    return similarity;
  }

  /**
   * Expert: sets the merge scheduler used by this writer. The default is
   * {@link ConcurrentMergeScheduler}.
   * <p>
   * <b>NOTE:</b> the merge scheduler cannot be null.
   *
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setMergeScheduler(MergeScheduler mergeScheduler) {
    if (mergeScheduler == null) {
      throw new IllegalArgumentException("mergeScheduler must not be null");
    }
    this.mergeScheduler = mergeScheduler;
    return this;
  }

  @Override
  public MergeScheduler getMergeScheduler() {
    return mergeScheduler;
  }

  /**
   * Sets the maximum time to wait for a write lock (in milliseconds) for this
   * instance. Note that the value can be zero, for no sleep/retry behavior.
   *
   * <p>Only takes effect when IndexWriter is first created.
   * @deprecated Use {@link SleepingLockWrapper} if you want sleeping.
   */
  @Deprecated
  public IndexWriterConfig setWriteLockTimeout(long writeLockTimeout) {
    this.writeLockTimeout = writeLockTimeout;
    return this;
  }

  @Override
  public long getWriteLockTimeout() {
    return writeLockTimeout;
  }

  /**
   * Set the {@link Codec}.
   * 
   * <p>
   * Only takes effect when IndexWriter is first created.
   */
  public IndexWriterConfig setCodec(Codec codec) {
    if (codec == null) {
      throw new IllegalArgumentException("codec must not be null");
    }
    this.codec = codec;
    return this;
  }

  @Override
  public Codec getCodec() {
    return codec;
  }


  @Override
  public MergePolicy getMergePolicy() {
    return mergePolicy;
  }

  /** Expert: Sets the {@link DocumentsWriterPerThreadPool} instance used by the
   * IndexWriter to assign thread-states to incoming indexing threads.
   * <p>
   * NOTE: The given {@link DocumentsWriterPerThreadPool} instance must not be used with
   * other {@link IndexWriter} instances once it has been initialized / associated with an
   * {@link IndexWriter}.
   * </p>
   * <p>
   * NOTE: This only takes effect when IndexWriter is first created.</p>*/
  IndexWriterConfig setIndexerThreadPool(DocumentsWriterPerThreadPool threadPool) {
    if (threadPool == null) {
      throw new IllegalArgumentException("threadPool must not be null");
    }
    this.indexerThreadPool = threadPool;
    return this;
  }

  @Override
  DocumentsWriterPerThreadPool getIndexerThreadPool() {
    return indexerThreadPool;
  }

  /** By default, IndexWriter does not pool the
   *  SegmentReaders it must open for deletions and
   *  merging, unless a near-real-time reader has been
   *  obtained by calling {@link DirectoryReader#open(IndexWriter)}.
   *  This method lets you enable pooling without getting a
   *  near-real-time reader.  NOTE: if you set this to
   *  false, IndexWriter will still pool readers once
   *  {@link DirectoryReader#open(IndexWriter)} is called.
   *
   * <p>Only takes effect when IndexWriter is first created. */
  public IndexWriterConfig setReaderPooling(boolean readerPooling) {
    this.readerPooling = readerPooling;
    return this;
  }

  @Override
  public boolean getReaderPooling() {
    return readerPooling;
  }

  /**
   * Expert: Controls when segments are flushed to disk during indexing.
   * The {@link FlushPolicy} initialized during {@link IndexWriter} instantiation and once initialized
   * the given instance is bound to this {@link IndexWriter} and should not be used with another writer.
   * @see #setMaxBufferedDeleteTerms(int)
   * @see #setMaxBufferedDocs(int)
   * @see #setRAMBufferSizeMB(double)
   */
  IndexWriterConfig setFlushPolicy(FlushPolicy flushPolicy) {
    if (flushPolicy == null) {
      throw new IllegalArgumentException("flushPolicy must not be null");
    }
    this.flushPolicy = flushPolicy;
    return this;
  }

  /**
   * Expert: Sets the maximum memory consumption per thread triggering a forced
   * flush if exceeded. A {@link DocumentsWriterPerThread} is forcefully flushed
   * once it exceeds this limit even if the {@link #getRAMBufferSizeMB()} has
   * not been exceeded. This is a safety limit to prevent a
   * {@link DocumentsWriterPerThread} from address space exhaustion due to its
   * internal 32 bit signed integer based memory addressing.
   * The given value must be less that 2GB (2048MB)
   * 
   * @see #DEFAULT_RAM_PER_THREAD_HARD_LIMIT_MB
   */
  public IndexWriterConfig setRAMPerThreadHardLimitMB(int perThreadHardLimitMB) {
    if (perThreadHardLimitMB <= 0 || perThreadHardLimitMB >= 2048) {
      throw new IllegalArgumentException("PerThreadHardLimit must be greater than 0 and less than 2048MB");
    }
    this.perThreadHardLimitMB = perThreadHardLimitMB;
    return this;
  }

  @Override
  public int getRAMPerThreadHardLimitMB() {
    return perThreadHardLimitMB;
  }
  
  @Override
  FlushPolicy getFlushPolicy() {
    return flushPolicy;
  }
  
  @Override
  public InfoStream getInfoStream() {
    return infoStream;
  }
  
  @Override
  public Analyzer getAnalyzer() {
    return super.getAnalyzer();
  }
  
  @Override
  public int getMaxBufferedDeleteTerms() {
    return super.getMaxBufferedDeleteTerms();
  }
  
  @Override
  public int getMaxBufferedDocs() {
    return super.getMaxBufferedDocs();
  }
  
  @Override
  public IndexReaderWarmer getMergedSegmentWarmer() {
    return super.getMergedSegmentWarmer();
  }
  
  @Override
  public double getRAMBufferSizeMB() {
    return super.getRAMBufferSizeMB();
  }
  
  /** 
   * Information about merges, deletes and a
   * message when maxFieldLength is reached will be printed
   * to this. Must not be null, but {@link InfoStream#NO_OUTPUT} 
   * may be used to supress output.
   */
  public IndexWriterConfig setInfoStream(InfoStream infoStream) {
    if (infoStream == null) {
      throw new IllegalArgumentException("Cannot set InfoStream implementation to null. "+
        "To disable logging use InfoStream.NO_OUTPUT");
    }
    this.infoStream = infoStream;
    return this;
  }
  
  /** 
   * Convenience method that uses {@link PrintStreamInfoStream}.  Must not be null.
   */
  public IndexWriterConfig setInfoStream(PrintStream printStream) {
    if (printStream == null) {
      throw new IllegalArgumentException("printStream must not be null");
    }
    return setInfoStream(new PrintStreamInfoStream(printStream));
  }
  
  @Override
  public IndexWriterConfig setMergePolicy(MergePolicy mergePolicy) {
    return (IndexWriterConfig) super.setMergePolicy(mergePolicy);
  }
  
  @Override
  public IndexWriterConfig setMaxBufferedDeleteTerms(int maxBufferedDeleteTerms) {
    return (IndexWriterConfig) super.setMaxBufferedDeleteTerms(maxBufferedDeleteTerms);
  }
  
  @Override
  public IndexWriterConfig setMaxBufferedDocs(int maxBufferedDocs) {
    return (IndexWriterConfig) super.setMaxBufferedDocs(maxBufferedDocs);
  }
  
  @Override
  public IndexWriterConfig setMergedSegmentWarmer(IndexReaderWarmer mergeSegmentWarmer) {
    return (IndexWriterConfig) super.setMergedSegmentWarmer(mergeSegmentWarmer);
  }
  
  @Override
  public IndexWriterConfig setRAMBufferSizeMB(double ramBufferSizeMB) {
    return (IndexWriterConfig) super.setRAMBufferSizeMB(ramBufferSizeMB);
  }
  
  @Override
  public IndexWriterConfig setUseCompoundFile(boolean useCompoundFile) {
    return (IndexWriterConfig) super.setUseCompoundFile(useCompoundFile);
  }

  /**
   * Sets if calls {@link IndexWriter#close()} should first commit
   * before closing.  Use <code>true</code> to match behavior of Lucene 4.x.
   */
  public IndexWriterConfig setCommitOnClose(boolean commitOnClose) {
    this.commitOnClose = commitOnClose;
    return this;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    sb.append("writer=").append(writer.get()).append("\n");
    return sb.toString();
  }
  
}
