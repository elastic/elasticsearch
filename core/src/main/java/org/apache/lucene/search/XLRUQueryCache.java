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
package org.apache.lucene.search;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TieredMergePolicy;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.RoaringDocIdSet;

/**
 * A {@link QueryCache} that evicts queries using a LRU (least-recently-used)
 * eviction policy in order to remain under a given maximum size and number of
 * bytes used.
 *
 * This class is thread-safe.
 *
 * Note that query eviction runs in linear time with the total number of
 * segments that have cache entries so this cache works best with
 * {@link QueryCachingPolicy caching policies} that only cache on "large"
 * segments, and it is advised to not share this cache across too many indices.
 *
 * A default query cache and policy instance is used in IndexSearcher. If you want to replace those defaults
 * it is typically done like this:
 * <pre class="prettyprint">
 *   final int maxNumberOfCachedQueries = 256;
 *   final long maxRamBytesUsed = 50 * 1024L * 1024L; // 50MB
 *   // these cache and policy instances can be shared across several queries and readers
 *   // it is fine to eg. store them into static variables
 *   final QueryCache queryCache = new XLRUQueryCache(maxNumberOfCachedQueries, maxRamBytesUsed);
 *   final QueryCachingPolicy defaultCachingPolicy = new UsageTrackingQueryCachingPolicy();
 *   indexSearcher.setQueryCache(queryCache);
 *   indexSearcher.setQueryCachingPolicy(defaultCachingPolicy);
 * </pre>
 *
 * This cache exposes some global statistics ({@link #getHitCount() hit count},
 * {@link #getMissCount() miss count}, {@link #getCacheSize() number of cache
 * entries}, {@link #getCacheCount() total number of DocIdSets that have ever
 * been cached}, {@link #getEvictionCount() number of evicted entries}). In
 * case you would like to have more fine-grained statistics, such as per-index
 * or per-query-class statistics, it is possible to override various callbacks:
 * {@link #onHit}, {@link #onMiss},
 * {@link #onQueryCache}, {@link #onQueryEviction},
 * {@link #onDocIdSetCache}, {@link #onDocIdSetEviction} and {@link #onClear}.
 * It is better to not perform heavy computations in these methods though since
 * they are called synchronously and under a lock.
 *
 * @see QueryCachingPolicy
 */
public class XLRUQueryCache implements QueryCache, Accountable {

  // memory usage of a simple term query
  static final long QUERY_DEFAULT_RAM_BYTES_USED = 192;

  static final long HASHTABLE_RAM_BYTES_PER_ENTRY =
      2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF // key + value
      * 2; // hash tables need to be oversized to avoid collisions, assume 2x capacity

  static final long LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY =
      HASHTABLE_RAM_BYTES_PER_ENTRY
      + 2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF; // previous & next references

  private final int maxSize;
  private final long maxRamBytesUsed;
  private final Predicate<LeafReaderContext> leavesToCache;
  // maps queries that are contained in the cache to a singleton so that this
  // cache does not store several copies of the same query
  private final Map<Query, Query> uniqueQueries;
  // The contract between this set and the per-leaf caches is that per-leaf caches
  // are only allowed to store sub-sets of the queries that are contained in
  // mostRecentlyUsedQueries. This is why write operations are performed under a lock
  private final Set<Query> mostRecentlyUsedQueries;
  private final Map<Object, LeafCache> cache;
  private final ReentrantLock lock;

  // these variables are volatile so that we do not need to sync reads
  // but increments need to be performed under the lock
  private volatile long ramBytesUsed;
  private volatile long hitCount;
  private volatile long missCount;
  private volatile long cacheCount;
  private volatile long cacheSize;

  /**
   * Expert: Create a new instance that will cache at most <code>maxSize</code>
   * queries with at most <code>maxRamBytesUsed</code> bytes of memory, only on
   * leaves that satisfy {@code leavesToCache};
   */
  public XLRUQueryCache(int maxSize, long maxRamBytesUsed,
      Predicate<LeafReaderContext> leavesToCache) {
    this.maxSize = maxSize;
    this.maxRamBytesUsed = maxRamBytesUsed;
    this.leavesToCache = leavesToCache;
    uniqueQueries = new LinkedHashMap<>(16, 0.75f, true);
    mostRecentlyUsedQueries = uniqueQueries.keySet();
    cache = new IdentityHashMap<>();
    lock = new ReentrantLock();
    ramBytesUsed = 0;
  }

  /**
   * Create a new instance that will cache at most <code>maxSize</code> queries
   * with at most <code>maxRamBytesUsed</code> bytes of memory. Queries will
   * only be cached on leaves that have more than 10k documents and have more
   * than 3% of the total number of documents in the index.
   * This should guarantee that all leaves from the upper
   * {@link TieredMergePolicy tier} will be cached while ensuring that at most
   * <tt>33</tt> leaves can make it to the cache (very likely less than 10 in
   * practice), which is useful for this implementation since some operations
   * perform in linear time with the number of cached leaves.
   */
  public XLRUQueryCache(int maxSize, long maxRamBytesUsed) {
    this(maxSize, maxRamBytesUsed, new MinSegmentSizePredicate(10000, .03f));
  }

  // pkg-private for testing
  static class MinSegmentSizePredicate implements Predicate<LeafReaderContext> {
    private final int minSize;
    private final float minSizeRatio;

    MinSegmentSizePredicate(int minSize, float minSizeRatio) {
      this.minSize = minSize;
      this.minSizeRatio = minSizeRatio;
    }

    @Override
    public boolean test(LeafReaderContext context) {
      final int maxDoc = context.reader().maxDoc();
      if (maxDoc < minSize) {
        return false;
      }
      final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(context);
      final float sizeRatio = (float) context.reader().maxDoc() / topLevelContext.reader().maxDoc();
      return sizeRatio >= minSizeRatio;
    }
  }

  /**
   * Expert: callback when there is a cache hit on a given query.
   * Implementing this method is typically useful in order to compute more
   * fine-grained statistics about the query cache.
   * @see #onMiss
   */
  protected void onHit(Object readerCoreKey, Query query) {
    assert lock.isHeldByCurrentThread();
    hitCount += 1;
  }

  /**
   * Expert: callback when there is a cache miss on a given query.
   * @see #onHit
   */
  protected void onMiss(Object readerCoreKey, Query query) {
    assert lock.isHeldByCurrentThread();
    assert query != null;
    missCount += 1;
  }

  /**
   * Expert: callback when a query is added to this cache.
   * Implementing this method is typically useful in order to compute more
   * fine-grained statistics about the query cache.
   * @see #onQueryEviction
   */
  protected void onQueryCache(Query query, long ramBytesUsed) {
    assert lock.isHeldByCurrentThread();
    this.ramBytesUsed += ramBytesUsed;
  }

  /**
   * Expert: callback when a query is evicted from this cache.
   * @see #onQueryCache
   */
  protected void onQueryEviction(Query query, long ramBytesUsed) {
    assert lock.isHeldByCurrentThread();
    this.ramBytesUsed -= ramBytesUsed;
  }

  /**
   * Expert: callback when a {@link DocIdSet} is added to this cache.
   * Implementing this method is typically useful in order to compute more
   * fine-grained statistics about the query cache.
   * @see #onDocIdSetEviction
   */
  protected void onDocIdSetCache(Object readerCoreKey, long ramBytesUsed) {
    assert lock.isHeldByCurrentThread();
    cacheSize += 1;
    cacheCount += 1;
    this.ramBytesUsed += ramBytesUsed;
  }

  /**
   * Expert: callback when one or more {@link DocIdSet}s are removed from this
   * cache.
   * @see #onDocIdSetCache
   */
  protected void onDocIdSetEviction(Object readerCoreKey, int numEntries, long sumRamBytesUsed) {
    assert lock.isHeldByCurrentThread();
    this.ramBytesUsed -= sumRamBytesUsed;
    cacheSize -= numEntries;
  }

  /**
   * Expert: callback when the cache is completely cleared.
   */
  protected void onClear() {
    assert lock.isHeldByCurrentThread();
    ramBytesUsed = 0;
    cacheSize = 0;
  }

  /** Whether evictions are required. */
  boolean requiresEviction() {
    assert lock.isHeldByCurrentThread();
    final int size = mostRecentlyUsedQueries.size();
    if (size == 0) {
      return false;
    } else {
      return size > maxSize || ramBytesUsed() > maxRamBytesUsed;
    }
  }

  DocIdSet get(Query key, LeafReaderContext context) {
    assert lock.isHeldByCurrentThread();
    assert key instanceof BoostQuery == false;
    assert key instanceof ConstantScoreQuery == false;
    final Object readerKey = context.reader().getCoreCacheKey();
    final LeafCache leafCache = cache.get(readerKey);
    if (leafCache == null) {
      onMiss(readerKey, key);
      return null;
    }
    // this get call moves the query to the most-recently-used position
    final Query singleton = uniqueQueries.get(key);
    if (singleton == null) {
      onMiss(readerKey, key);
      return null;
    }
    final DocIdSet cached = leafCache.get(singleton);
    if (cached == null) {
      onMiss(readerKey, singleton);
    } else {
      onHit(readerKey, singleton);
    }
    return cached;
  }

  void putIfAbsent(Query query, LeafReaderContext context, DocIdSet set) {
    assert query instanceof BoostQuery == false;
    assert query instanceof ConstantScoreQuery == false;
    // under a lock to make sure that mostRecentlyUsedQueries and cache remain sync'ed
    lock.lock();
    try {
      Query singleton = uniqueQueries.putIfAbsent(query, query);
      if (singleton == null) {
        onQueryCache(singleton, LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY + ramBytesUsed(query));
      } else {
        query = singleton;
      }
      final Object key = context.reader().getCoreCacheKey();
      LeafCache leafCache = cache.get(key);
      if (leafCache == null) {
        leafCache = new LeafCache(key);
        final LeafCache previous = cache.put(context.reader().getCoreCacheKey(), leafCache);
        ramBytesUsed += HASHTABLE_RAM_BYTES_PER_ENTRY;
        assert previous == null;
        // we just created a new leaf cache, need to register a close listener
        context.reader().addCoreClosedListener(this::clearCoreCacheKey);
      }
      leafCache.putIfAbsent(query, set);
      evictIfNecessary();
    } finally {
      lock.unlock();
    }
  }

  void evictIfNecessary() {
    assert lock.isHeldByCurrentThread();
    // under a lock to make sure that mostRecentlyUsedQueries and cache keep sync'ed
    if (requiresEviction()) {

      Iterator<Query> iterator = mostRecentlyUsedQueries.iterator();
      do {
        final Query query = iterator.next();
        final int size = mostRecentlyUsedQueries.size();
        iterator.remove();
        if (size == mostRecentlyUsedQueries.size()) {
          // size did not decrease, because the hash of the query changed since it has been
          // put into the cache
          throw new ConcurrentModificationException("Removal from the cache failed! This " +
              "is probably due to a query which has been modified after having been put into " +
              " the cache or a badly implemented clone(). Query class: [" + query.getClass() +
              "], query: [" + query + "]");
        }
        onEviction(query);
      } while (iterator.hasNext() && requiresEviction());
    }
  }

  /**
   * Remove all cache entries for the given core cache key.
   */
  public void clearCoreCacheKey(Object coreKey) {
    lock.lock();
    try {
      final LeafCache leafCache = cache.remove(coreKey);
      if (leafCache != null) {
        ramBytesUsed -= HASHTABLE_RAM_BYTES_PER_ENTRY;
        final int numEntries = leafCache.cache.size();
        if (numEntries > 0) {
          onDocIdSetEviction(coreKey, numEntries, leafCache.ramBytesUsed);
        } else {
          assert numEntries == 0;
          assert leafCache.ramBytesUsed == 0;
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Remove all cache entries for the given query.
   */
  public void clearQuery(Query query) {
    lock.lock();
    try {
      final Query singleton = uniqueQueries.remove(query);
      if (singleton != null) {
        onEviction(singleton);
      }
    } finally {
      lock.unlock();
    }
  }

  private void onEviction(Query singleton) {
    assert lock.isHeldByCurrentThread();
    onQueryEviction(singleton, LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY + ramBytesUsed(singleton));
    for (LeafCache leafCache : cache.values()) {
      leafCache.remove(singleton);
    }
  }

  /**
   * Clear the content of this cache.
   */
  public void clear() {
    lock.lock();
    try {
      cache.clear();
      // Note that this also clears the uniqueQueries map since mostRecentlyUsedQueries is the uniqueQueries.keySet view:
      mostRecentlyUsedQueries.clear();
      onClear();
    } finally {
      lock.unlock();
    }
  }

  // pkg-private for testing
  void assertConsistent() {
    lock.lock();
    try {
      if (requiresEviction()) {
        throw new AssertionError("requires evictions: size=" + mostRecentlyUsedQueries.size()
            + ", maxSize=" + maxSize + ", ramBytesUsed=" + ramBytesUsed() + ", maxRamBytesUsed=" + maxRamBytesUsed);
      }
      for (LeafCache leafCache : cache.values()) {
        Set<Query> keys = Collections.newSetFromMap(new IdentityHashMap<>());
        keys.addAll(leafCache.cache.keySet());
        keys.removeAll(mostRecentlyUsedQueries);
        if (!keys.isEmpty()) {
          throw new AssertionError("One leaf cache contains more keys than the top-level cache: " + keys);
        }
      }
      long recomputedRamBytesUsed =
            HASHTABLE_RAM_BYTES_PER_ENTRY * cache.size()
          + LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY * uniqueQueries.size();
      for (Query query : mostRecentlyUsedQueries) {
        recomputedRamBytesUsed += ramBytesUsed(query);
      }
      for (LeafCache leafCache : cache.values()) {
        recomputedRamBytesUsed += HASHTABLE_RAM_BYTES_PER_ENTRY * leafCache.cache.size();
        for (DocIdSet set : leafCache.cache.values()) {
          recomputedRamBytesUsed += set.ramBytesUsed();
        }
      }
      if (recomputedRamBytesUsed != ramBytesUsed) {
        throw new AssertionError("ramBytesUsed mismatch : " + ramBytesUsed + " != " + recomputedRamBytesUsed);
      }

      long recomputedCacheSize = 0;
      for (LeafCache leafCache : cache.values()) {
        recomputedCacheSize += leafCache.cache.size();
      }
      if (recomputedCacheSize != getCacheSize()) {
        throw new AssertionError("cacheSize mismatch : " + getCacheSize() + " != " + recomputedCacheSize);
      }
    } finally {
      lock.unlock();
    }
  }

  // pkg-private for testing
  // return the list of cached queries in LRU order
  List<Query> cachedQueries() {
    lock.lock();
    try {
      return new ArrayList<>(mostRecentlyUsedQueries);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Weight doCache(Weight weight, QueryCachingPolicy policy) {
    while (weight instanceof CachingWrapperWeight) {
      weight = ((CachingWrapperWeight) weight).in;
    }

    return new CachingWrapperWeight(weight, policy);
  }

  @Override
  public long ramBytesUsed() {
    return ramBytesUsed;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    lock.lock();
    try {
      return Accountables.namedAccountables("segment", cache);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Return the number of bytes used by the given query. The default
   * implementation returns {@link Accountable#ramBytesUsed()} if the query
   * implements {@link Accountable} and <code>1024</code> otherwise.
   */
  protected long ramBytesUsed(Query query) {
    if (query instanceof Accountable) {
      return ((Accountable) query).ramBytesUsed();
    }
    return QUERY_DEFAULT_RAM_BYTES_USED;
  }

  /**
   * Default cache implementation: uses {@link RoaringDocIdSet} for sets that
   * have a density &lt; 1% and a {@link BitDocIdSet} over a {@link FixedBitSet}
   * otherwise.
   */
  protected DocIdSet cacheImpl(BulkScorer scorer, int maxDoc) throws IOException {
    if (scorer.cost() * 100 >= maxDoc) {
      // FixedBitSet is faster for dense sets and will enable the random-access
      // optimization in ConjunctionDISI
      return cacheIntoBitSet(scorer, maxDoc);
    } else {
      return cacheIntoRoaringDocIdSet(scorer, maxDoc);
    }
  }

  private static DocIdSet cacheIntoBitSet(BulkScorer scorer, int maxDoc) throws IOException {
    final FixedBitSet bitSet = new FixedBitSet(maxDoc);
    long cost[] = new long[1];
    scorer.score(new LeafCollector() {

      @Override
      public void setScorer(Scorer scorer) throws IOException {}

      @Override
      public void collect(int doc) throws IOException {
        cost[0]++;
        bitSet.set(doc);
      }

    }, null);
    return new BitDocIdSet(bitSet, cost[0]);
  }

  private static DocIdSet cacheIntoRoaringDocIdSet(BulkScorer scorer, int maxDoc) throws IOException {
    RoaringDocIdSet.Builder builder = new RoaringDocIdSet.Builder(maxDoc);
    scorer.score(new LeafCollector() {

      @Override
      public void setScorer(Scorer scorer) throws IOException {}

      @Override
      public void collect(int doc) throws IOException {
        builder.add(doc);
      }

    }, null);
    return builder.build();
  }

  /**
   * Return the total number of times that a {@link Query} has been looked up
   * in this {@link QueryCache}. Note that this number is incremented once per
   * segment so running a cached query only once will increment this counter
   * by the number of segments that are wrapped by the searcher.
   * Note that by definition, {@link #getTotalCount()} is the sum of
   * {@link #getHitCount()} and {@link #getMissCount()}.
   * @see #getHitCount()
   * @see #getMissCount()
   */
  public final long getTotalCount() {
    return getHitCount() + getMissCount();
  }

  /**
   * Over the {@link #getTotalCount() total} number of times that a query has
   * been looked up, return how many times a cached {@link DocIdSet} has been
   * found and returned.
   * @see #getTotalCount()
   * @see #getMissCount()
   */
  public final long getHitCount() {
    return hitCount;
  }

  /**
   * Over the {@link #getTotalCount() total} number of times that a query has
   * been looked up, return how many times this query was not contained in the
   * cache.
   * @see #getTotalCount()
   * @see #getHitCount()
   */
  public final long getMissCount() {
    return missCount;
  }

  /**
   * Return the total number of {@link DocIdSet}s which are currently stored
   * in the cache.
   * @see #getCacheCount()
   * @see #getEvictionCount()
   */
  public final long getCacheSize() {
    return cacheSize;
  }

  /**
   * Return the total number of cache entries that have been generated and put
   * in the cache. It is highly desirable to have a {@link #getHitCount() hit
   * count} that is much higher than the {@link #getCacheCount() cache count}
   * as the opposite would indicate that the query cache makes efforts in order
   * to cache queries but then they do not get reused.
   * @see #getCacheSize()
   * @see #getEvictionCount()
   */
  public final long getCacheCount() {
    return cacheCount;
  }

  /**
   * Return the number of cache entries that have been removed from the cache
   * either in order to stay under the maximum configured size/ram usage, or
   * because a segment has been closed. High numbers of evictions might mean
   * that queries are not reused or that the {@link QueryCachingPolicy
   * caching policy} caches too aggressively on NRT segments which get merged
   * early.
   * @see #getCacheCount()
   * @see #getCacheSize()
   */
  public final long getEvictionCount() {
    return getCacheCount() - getCacheSize();
  }

  // this class is not thread-safe, everything but ramBytesUsed needs to be called under a lock
  private class LeafCache implements Accountable {

    private final Object key;
    private final Map<Query, DocIdSet> cache;
    private volatile long ramBytesUsed;

    LeafCache(Object key) {
      this.key = key;
      cache = new IdentityHashMap<>();
      ramBytesUsed = 0;
    }

    private void onDocIdSetCache(long ramBytesUsed) {
      this.ramBytesUsed += ramBytesUsed;
      XLRUQueryCache.this.onDocIdSetCache(key, ramBytesUsed);
    }

    private void onDocIdSetEviction(long ramBytesUsed) {
      this.ramBytesUsed -= ramBytesUsed;
      XLRUQueryCache.this.onDocIdSetEviction(key, 1, ramBytesUsed);
    }

    DocIdSet get(Query query) {
      assert query instanceof BoostQuery == false;
      assert query instanceof ConstantScoreQuery == false;
      return cache.get(query);
    }

    void putIfAbsent(Query query, DocIdSet set) {
      assert query instanceof BoostQuery == false;
      assert query instanceof ConstantScoreQuery == false;
      if (cache.putIfAbsent(query, set) == null) {
        // the set was actually put
        onDocIdSetCache(HASHTABLE_RAM_BYTES_PER_ENTRY + set.ramBytesUsed());
      }
    }

    void remove(Query query) {
      assert query instanceof BoostQuery == false;
      assert query instanceof ConstantScoreQuery == false;
      DocIdSet removed = cache.remove(query);
      if (removed != null) {
        onDocIdSetEviction(HASHTABLE_RAM_BYTES_PER_ENTRY + removed.ramBytesUsed());
      }
    }

    @Override
    public long ramBytesUsed() {
      return ramBytesUsed;
    }

  }

  private class CachingWrapperWeight extends ConstantScoreWeight {

    private final Weight in;
    private final QueryCachingPolicy policy;
    // we use an AtomicBoolean because Weight.scorer may be called from multiple
    // threads when IndexSearcher is created with threads
    private final AtomicBoolean used;

    CachingWrapperWeight(Weight in, QueryCachingPolicy policy) {
      super(in.getQuery());
      this.in = in;
      this.policy = policy;
      used = new AtomicBoolean(false);
    }

    @Override
    public void extractTerms(Set<Term> terms) {
      in.extractTerms(terms);
    }

    private boolean cacheEntryHasReasonableWorstCaseSize(int maxDoc) {
      // The worst-case (dense) is a bit set which needs one bit per document
      final long worstCaseRamUsage = maxDoc / 8;
      final long totalRamAvailable = maxRamBytesUsed;
      // Imagine the worst-case that a cache entry is large than the size of
      // the cache: not only will this entry be trashed immediately but it
      // will also evict all current entries from the cache. For this reason
      // we only cache on an IndexReader if we have available room for
      // 5 different filters on this reader to avoid excessive trashing
      return worstCaseRamUsage * 5 < totalRamAvailable;
    }

    private DocIdSet cache(LeafReaderContext context) throws IOException {
      final BulkScorer scorer = in.bulkScorer(context);
      if (scorer == null) {
        return DocIdSet.EMPTY;
      } else {
        return cacheImpl(scorer, context.reader().maxDoc());
      }
    }

    /** Check whether this segment is eligible for caching, regardless of the query. */
    private boolean shouldCache(LeafReaderContext context) throws IOException {
      return cacheEntryHasReasonableWorstCaseSize(ReaderUtil.getTopLevelContext(context).reader().maxDoc())
          && leavesToCache.test(context);
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
      if (used.compareAndSet(false, true)) {
        policy.onUse(getQuery());
      }
      // Short-circuit: Check whether this segment is eligible for caching
      // before we take a lock because of #get
      if (shouldCache(context) == false) {
        return in.scorerSupplier(context);
      }

      // If the lock is already busy, prefer using the uncached version than waiting
      if (lock.tryLock() == false) {
        return in.scorerSupplier(context);
      }

      DocIdSet docIdSet;
      try {
        docIdSet = get(in.getQuery(), context);
      } finally {
        lock.unlock();
      }

      if (docIdSet == null) {
        if (policy.shouldCache(in.getQuery())) {
          docIdSet = cache(context);
          putIfAbsent(in.getQuery(), context, docIdSet);
        } else {
          return in.scorerSupplier(context);
        }
      }

      assert docIdSet != null;
      if (docIdSet == DocIdSet.EMPTY) {
        return null;
      }
      final DocIdSetIterator disi = docIdSet.iterator();
      if (disi == null) {
        return null;
      }

      return new ScorerSupplier() {
        @Override
        public Scorer get(boolean randomAccess) throws IOException {
          return new ConstantScoreScorer(CachingWrapperWeight.this, 0f, disi);
        }
        
        @Override
        public long cost() {
          return disi.cost();
        }
      };

    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      ScorerSupplier scorerSupplier = scorerSupplier(context);
      if (scorerSupplier == null) {
        return null;
      }
      return scorerSupplier.get(false);
    }

    @Override
    public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
      if (used.compareAndSet(false, true)) {
        policy.onUse(getQuery());
      }
      // Short-circuit: Check whether this segment is eligible for caching
      // before we take a lock because of #get
      if (shouldCache(context) == false) {
        return in.bulkScorer(context);
      }

      // If the lock is already busy, prefer using the uncached version than waiting
      if (lock.tryLock() == false) {
        return in.bulkScorer(context);
      }

      DocIdSet docIdSet;
      try {
        docIdSet = get(in.getQuery(), context);
      } finally {
        lock.unlock();
      }

      if (docIdSet == null) {
        if (policy.shouldCache(in.getQuery())) {
          docIdSet = cache(context);
          putIfAbsent(in.getQuery(), context, docIdSet);
        } else {
          return in.bulkScorer(context);
        }
      }

      assert docIdSet != null;
      if (docIdSet == DocIdSet.EMPTY) {
        return null;
      }
      final DocIdSetIterator disi = docIdSet.iterator();
      if (disi == null) {
        return null;
      }

      return new DefaultBulkScorer(new ConstantScoreScorer(this, 0f, disi));
    }

  }
}
