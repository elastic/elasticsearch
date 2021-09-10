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


import org.apache.lucene5_shaded.analysis.Analyzer;
import org.apache.lucene5_shaded.codecs.Codec;
import org.apache.lucene5_shaded.index.DocumentsWriterPerThread.IndexingChain;
import org.apache.lucene5_shaded.index.IndexWriter.IndexReaderWarmer;
import org.apache.lucene5_shaded.index.IndexWriterConfig.OpenMode;
import org.apache.lucene5_shaded.search.IndexSearcher;
import org.apache.lucene5_shaded.search.similarities.Similarity;
import org.apache.lucene5_shaded.store.SleepingLockWrapper;
import org.apache.lucene5_shaded.util.InfoStream;

/**
 * Holds all the configuration used by {@link IndexWriter} with few setters for
 * settings that can be changed on an {@link IndexWriter} instance "live".
 * 
 * @since 4.0
 */
public class LiveIndexWriterConfig {
  
  private final Analyzer analyzer;
  
  private volatile int maxBufferedDocs;
  private volatile double ramBufferSizeMB;
  private volatile int maxBufferedDeleteTerms;
  private volatile IndexReaderWarmer mergedSegmentWarmer;

  // modified by IndexWriterConfig
  /** {@link IndexDeletionPolicy} controlling when commit
   *  points are deleted. */
  protected volatile IndexDeletionPolicy delPolicy;

  /** {@link IndexCommit} that {@link IndexWriter} is
   *  opened on. */
  protected volatile IndexCommit commit;

  /** {@link OpenMode} that {@link IndexWriter} is opened
   *  with. */
  protected volatile OpenMode openMode;

  /** {@link Similarity} to use when encoding norms. */
  protected volatile Similarity similarity;

  /** {@link MergeScheduler} to use for running merges. */
  protected volatile MergeScheduler mergeScheduler;

  /** 
   * Timeout when trying to obtain the write lock on init. 
   * @deprecated Use {@link SleepingLockWrapper} if you want sleeping.
   */
  @Deprecated
  protected volatile long writeLockTimeout;

  /** {@link IndexingChain} that determines how documents are
   *  indexed. */
  protected volatile IndexingChain indexingChain;

  /** {@link Codec} used to write new segments. */
  protected volatile Codec codec;

  /** {@link InfoStream} for debugging messages. */
  protected volatile InfoStream infoStream;

  /** {@link MergePolicy} for selecting merges. */
  protected volatile MergePolicy mergePolicy;

  /** {@code DocumentsWriterPerThreadPool} to control how
   *  threads are allocated to {@code DocumentsWriterPerThread}. */
  protected volatile DocumentsWriterPerThreadPool indexerThreadPool;

  /** True if readers should be pooled. */
  protected volatile boolean readerPooling;

  /** {@link FlushPolicy} to control when segments are
   *  flushed. */
  protected volatile FlushPolicy flushPolicy;

  /** Sets the hard upper bound on RAM usage for a single
   *  segment, after which the segment is forced to flush. */
  protected volatile int perThreadHardLimitMB;

  /** True if segment flushes should use compound file format */
  protected volatile boolean useCompoundFile = IndexWriterConfig.DEFAULT_USE_COMPOUND_FILE_SYSTEM;
  
  /** True if calls to {@link IndexWriter#close()} should first do a commit. */
  protected boolean commitOnClose = IndexWriterConfig.DEFAULT_COMMIT_ON_CLOSE;

  // used by IndexWriterConfig
  LiveIndexWriterConfig(Analyzer analyzer) {
    this.analyzer = analyzer;
    ramBufferSizeMB = IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB;
    maxBufferedDocs = IndexWriterConfig.DEFAULT_MAX_BUFFERED_DOCS;
    maxBufferedDeleteTerms = IndexWriterConfig.DEFAULT_MAX_BUFFERED_DELETE_TERMS;
    mergedSegmentWarmer = null;
    delPolicy = new KeepOnlyLastCommitDeletionPolicy();
    commit = null;
    useCompoundFile = IndexWriterConfig.DEFAULT_USE_COMPOUND_FILE_SYSTEM;
    openMode = OpenMode.CREATE_OR_APPEND;
    similarity = IndexSearcher.getDefaultSimilarity();
    mergeScheduler = new ConcurrentMergeScheduler();
    writeLockTimeout = IndexWriterConfig.WRITE_LOCK_TIMEOUT;
    indexingChain = DocumentsWriterPerThread.defaultIndexingChain;
    codec = Codec.getDefault();
    if (codec == null) {
      throw new NullPointerException();
    }
    infoStream = InfoStream.getDefault();
    mergePolicy = new TieredMergePolicy();
    flushPolicy = new FlushByRamOrCountsPolicy();
    readerPooling = IndexWriterConfig.DEFAULT_READER_POOLING;
    indexerThreadPool = new DocumentsWriterPerThreadPool();
    perThreadHardLimitMB = IndexWriterConfig.DEFAULT_RAM_PER_THREAD_HARD_LIMIT_MB;
  }
  
  /** Returns the default analyzer to use for indexing documents. */
  public Analyzer getAnalyzer() {
    return analyzer;
  }

  /**
   * Determines the maximum number of delete-by-term operations that will be
   * buffered before both the buffered in-memory delete terms and queries are
   * applied and flushed.
   * <p>
   * Disabled by default (writer flushes by RAM usage).
   * <p>
   * NOTE: This setting won't trigger a segment flush.
   * 
   * <p>
   * Takes effect immediately, but only the next time a document is added,
   * updated or deleted. Also, if you only delete-by-query, this setting has no
   * effect, i.e. delete queries are buffered until the next segment is flushed.
   * 
   * @throws IllegalArgumentException
   *           if maxBufferedDeleteTerms is enabled but smaller than 1
   * 
   * @see #setRAMBufferSizeMB
   */
  public LiveIndexWriterConfig setMaxBufferedDeleteTerms(int maxBufferedDeleteTerms) {
    if (maxBufferedDeleteTerms != IndexWriterConfig.DISABLE_AUTO_FLUSH && maxBufferedDeleteTerms < 1) {
      throw new IllegalArgumentException("maxBufferedDeleteTerms must at least be 1 when enabled");
    }
    this.maxBufferedDeleteTerms = maxBufferedDeleteTerms;
    return this;
  }

  /**
   * Returns the number of buffered deleted terms that will trigger a flush of all
   * buffered deletes if enabled.
   *
   * @see #setMaxBufferedDeleteTerms(int)
   */
  public int getMaxBufferedDeleteTerms() {
    return maxBufferedDeleteTerms;
  }
  
  /**
   * Determines the amount of RAM that may be used for buffering added documents
   * and deletions before they are flushed to the Directory. Generally for
   * faster indexing performance it's best to flush by RAM usage instead of
   * document count and use as large a RAM buffer as you can.
   * <p>
   * When this is set, the writer will flush whenever buffered documents and
   * deletions use this much RAM. Pass in
   * {@link IndexWriterConfig#DISABLE_AUTO_FLUSH} to prevent triggering a flush
   * due to RAM usage. Note that if flushing by document count is also enabled,
   * then the flush will be triggered by whichever comes first.
   * <p>
   * The maximum RAM limit is inherently determined by the JVMs available
   * memory. Yet, an {@link IndexWriter} session can consume a significantly
   * larger amount of memory than the given RAM limit since this limit is just
   * an indicator when to flush memory resident documents to the Directory.
   * Flushes are likely happen concurrently while other threads adding documents
   * to the writer. For application stability the available memory in the JVM
   * should be significantly larger than the RAM buffer used for indexing.
   * <p>
   * <b>NOTE</b>: the account of RAM usage for pending deletions is only
   * approximate. Specifically, if you delete by Query, Lucene currently has no
   * way to measure the RAM usage of individual Queries so the accounting will
   * under-estimate and you should compensate by either calling commit()
   * periodically yourself, or by using {@link #setMaxBufferedDeleteTerms(int)}
   * to flush and apply buffered deletes by count instead of RAM usage (for each
   * buffered delete Query a constant number of bytes is used to estimate RAM
   * usage). Note that enabling {@link #setMaxBufferedDeleteTerms(int)} will not
   * trigger any segment flushes.
   * <p>
   * <b>NOTE</b>: It's not guaranteed that all memory resident documents are
   * flushed once this limit is exceeded. Depending on the configured
   * {@link FlushPolicy} only a subset of the buffered documents are flushed and
   * therefore only parts of the RAM buffer is released.
   * <p>
   * 
   * The default value is {@link IndexWriterConfig#DEFAULT_RAM_BUFFER_SIZE_MB}.
   * 
   * <p>
   * Takes effect immediately, but only the next time a document is added,
   * updated or deleted.
   * 
   * @see IndexWriterConfig#setRAMPerThreadHardLimitMB(int)
   * 
   * @throws IllegalArgumentException
   *           if ramBufferSize is enabled but non-positive, or it disables
   *           ramBufferSize when maxBufferedDocs is already disabled
   */
  public synchronized LiveIndexWriterConfig setRAMBufferSizeMB(double ramBufferSizeMB) {
    if (ramBufferSizeMB != IndexWriterConfig.DISABLE_AUTO_FLUSH && ramBufferSizeMB <= 0.0) {
      throw new IllegalArgumentException("ramBufferSize should be > 0.0 MB when enabled");
    }
    if (ramBufferSizeMB == IndexWriterConfig.DISABLE_AUTO_FLUSH
        && maxBufferedDocs == IndexWriterConfig.DISABLE_AUTO_FLUSH) {
      throw new IllegalArgumentException("at least one of ramBufferSize and maxBufferedDocs must be enabled");
    }
    this.ramBufferSizeMB = ramBufferSizeMB;
    return this;
  }

  /** Returns the value set by {@link #setRAMBufferSizeMB(double)} if enabled. */
  public double getRAMBufferSizeMB() {
    return ramBufferSizeMB;
  }
  
  /**
   * Determines the minimal number of documents required before the buffered
   * in-memory documents are flushed as a new Segment. Large values generally
   * give faster indexing.
   * 
   * <p>
   * When this is set, the writer will flush every maxBufferedDocs added
   * documents. Pass in {@link IndexWriterConfig#DISABLE_AUTO_FLUSH} to prevent
   * triggering a flush due to number of buffered documents. Note that if
   * flushing by RAM usage is also enabled, then the flush will be triggered by
   * whichever comes first.
   * 
   * <p>
   * Disabled by default (writer flushes by RAM usage).
   * 
   * <p>
   * Takes effect immediately, but only the next time a document is added,
   * updated or deleted.
   * 
   * @see #setRAMBufferSizeMB(double)
   * @throws IllegalArgumentException
   *           if maxBufferedDocs is enabled but smaller than 2, or it disables
   *           maxBufferedDocs when ramBufferSize is already disabled
   */
  public synchronized LiveIndexWriterConfig setMaxBufferedDocs(int maxBufferedDocs) {
    if (maxBufferedDocs != IndexWriterConfig.DISABLE_AUTO_FLUSH && maxBufferedDocs < 2) {
      throw new IllegalArgumentException("maxBufferedDocs must at least be 2 when enabled");
    }
    if (maxBufferedDocs == IndexWriterConfig.DISABLE_AUTO_FLUSH
        && ramBufferSizeMB == IndexWriterConfig.DISABLE_AUTO_FLUSH) {
      throw new IllegalArgumentException("at least one of ramBufferSize and maxBufferedDocs must be enabled");
    }
    this.maxBufferedDocs = maxBufferedDocs;
    return this;
  }

  /**
   * Returns the number of buffered added documents that will trigger a flush if
   * enabled.
   *
   * @see #setMaxBufferedDocs(int)
   */
  public int getMaxBufferedDocs() {
    return maxBufferedDocs;
  }

  /**
   * Expert: {@link MergePolicy} is invoked whenever there are changes to the
   * segments in the index. Its role is to select which merges to do, if any,
   * and return a {@link MergePolicy.MergeSpecification} describing the merges.
   * It also selects merges to do for forceMerge.
   * 
   * <p>
   * Takes effect on subsequent merge selections. Any merges in flight or any
   * merges already registered by the previous {@link MergePolicy} are not
   * affected.
   */
  public LiveIndexWriterConfig setMergePolicy(MergePolicy mergePolicy) {
    if (mergePolicy == null) {
      throw new IllegalArgumentException("mergePolicy must not be null");
    }
    this.mergePolicy = mergePolicy;
    return this;
  }

  /**
   * Set the merged segment warmer. See {@link IndexReaderWarmer}.
   * 
   * <p>
   * Takes effect on the next merge.
   */
  public LiveIndexWriterConfig setMergedSegmentWarmer(IndexReaderWarmer mergeSegmentWarmer) {
    this.mergedSegmentWarmer = mergeSegmentWarmer;
    return this;
  }

  /** Returns the current merged segment warmer. See {@link IndexReaderWarmer}. */
  public IndexReaderWarmer getMergedSegmentWarmer() {
    return mergedSegmentWarmer;
  }
  
  /** Returns the {@link OpenMode} set by {@link IndexWriterConfig#setOpenMode(OpenMode)}. */
  public OpenMode getOpenMode() {
    return openMode;
  }
  
  /**
   * Returns the {@link IndexDeletionPolicy} specified in
   * {@link IndexWriterConfig#setIndexDeletionPolicy(IndexDeletionPolicy)} or
   * the default {@link KeepOnlyLastCommitDeletionPolicy}/
   */
  public IndexDeletionPolicy getIndexDeletionPolicy() {
    return delPolicy;
  }
  
  /**
   * Returns the {@link IndexCommit} as specified in
   * {@link IndexWriterConfig#setIndexCommit(IndexCommit)} or the default,
   * {@code null} which specifies to open the latest index commit point.
   */
  public IndexCommit getIndexCommit() {
    return commit;
  }

  /**
   * Expert: returns the {@link Similarity} implementation used by this
   * {@link IndexWriter}.
   */
  public Similarity getSimilarity() {
    return similarity;
  }
  
  /**
   * Returns the {@link MergeScheduler} that was set by
   * {@link IndexWriterConfig#setMergeScheduler(MergeScheduler)}.
   */
  public MergeScheduler getMergeScheduler() {
    return mergeScheduler;
  }

  /**
   * Returns allowed timeout when acquiring the write lock.
   *
   * @see IndexWriterConfig#setWriteLockTimeout(long)
   * @deprecated Use {@link SleepingLockWrapper} if you want sleeping.
   */
  @Deprecated
  public long getWriteLockTimeout() {
    return writeLockTimeout;
  }
  
  /** Returns the current {@link Codec}. */
  public Codec getCodec() {
    return codec;
  }

  /**
   * Returns the current MergePolicy in use by this writer.
   *
   * @see IndexWriterConfig#setMergePolicy(MergePolicy)
   */
  public MergePolicy getMergePolicy() {
    return mergePolicy;
  }
  
  /**
   * Returns the configured {@link DocumentsWriterPerThreadPool} instance.
   * 
   * @see IndexWriterConfig#setIndexerThreadPool(DocumentsWriterPerThreadPool)
   * @return the configured {@link DocumentsWriterPerThreadPool} instance.
   */
  DocumentsWriterPerThreadPool getIndexerThreadPool() {
    return indexerThreadPool;
  }

  /**
   * Returns {@code true} if {@link IndexWriter} should pool readers even if
   * {@link DirectoryReader#open(IndexWriter)} has not been called.
   */
  public boolean getReaderPooling() {
    return readerPooling;
  }

  /**
   * Returns the indexing chain.
   */
  IndexingChain getIndexingChain() {
    return indexingChain;
  }

  /**
   * Returns the max amount of memory each {@link DocumentsWriterPerThread} can
   * consume until forcefully flushed.
   * 
   * @see IndexWriterConfig#setRAMPerThreadHardLimitMB(int)
   */
  public int getRAMPerThreadHardLimitMB() {
    return perThreadHardLimitMB;
  }
  
  /**
   * @see IndexWriterConfig#setFlushPolicy(FlushPolicy)
   */
  FlushPolicy getFlushPolicy() {
    return flushPolicy;
  }
  
  /** Returns {@link InfoStream} used for debugging.
   *
   * @see IndexWriterConfig#setInfoStream(InfoStream)
   */
  public InfoStream getInfoStream() {
    return infoStream;
  }
  
  /**
   * Sets if the {@link IndexWriter} should pack newly written segments in a
   * compound file. Default is <code>true</code>.
   * <p>
   * Use <code>false</code> for batch indexing with very large ram buffer
   * settings.
   * </p>
   * <p>
   * <b>Note: To control compound file usage during segment merges see
   * {@link MergePolicy#setNoCFSRatio(double)} and
   * {@link MergePolicy#setMaxCFSSegmentSizeMB(double)}. This setting only
   * applies to newly created segments.</b>
   * </p>
   */
  public LiveIndexWriterConfig setUseCompoundFile(boolean useCompoundFile) {
    this.useCompoundFile = useCompoundFile;
    return this;
  }
  
  /**
   * Returns <code>true</code> iff the {@link IndexWriter} packs
   * newly written segments in a compound file. Default is <code>true</code>.
   */
  public boolean getUseCompoundFile() {
    return useCompoundFile ;
  }
  
  /**
   * Returns <code>true</code> if {@link IndexWriter#close()} should first commit before closing.
   */
  public boolean getCommitOnClose() {
    return commitOnClose;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("analyzer=").append(analyzer == null ? "null" : analyzer.getClass().getName()).append("\n");
    sb.append("ramBufferSizeMB=").append(getRAMBufferSizeMB()).append("\n");
    sb.append("maxBufferedDocs=").append(getMaxBufferedDocs()).append("\n");
    sb.append("maxBufferedDeleteTerms=").append(getMaxBufferedDeleteTerms()).append("\n");
    sb.append("mergedSegmentWarmer=").append(getMergedSegmentWarmer()).append("\n");
    sb.append("delPolicy=").append(getIndexDeletionPolicy().getClass().getName()).append("\n");
    IndexCommit commit = getIndexCommit();
    sb.append("commit=").append(commit == null ? "null" : commit).append("\n");
    sb.append("openMode=").append(getOpenMode()).append("\n");
    sb.append("similarity=").append(getSimilarity().getClass().getName()).append("\n");
    sb.append("mergeScheduler=").append(getMergeScheduler()).append("\n");
    sb.append("default WRITE_LOCK_TIMEOUT=").append(IndexWriterConfig.WRITE_LOCK_TIMEOUT).append("\n");
    sb.append("writeLockTimeout=").append(getWriteLockTimeout()).append("\n");
    sb.append("codec=").append(getCodec()).append("\n");
    sb.append("infoStream=").append(getInfoStream().getClass().getName()).append("\n");
    sb.append("mergePolicy=").append(getMergePolicy()).append("\n");
    sb.append("indexerThreadPool=").append(getIndexerThreadPool()).append("\n");
    sb.append("readerPooling=").append(getReaderPooling()).append("\n");
    sb.append("perThreadHardLimitMB=").append(getRAMPerThreadHardLimitMB()).append("\n");
    sb.append("useCompoundFile=").append(getUseCompoundFile()).append("\n");
    sb.append("commitOnClose=").append(getCommitOnClose()).append("\n");
    return sb.toString();
  }
}
