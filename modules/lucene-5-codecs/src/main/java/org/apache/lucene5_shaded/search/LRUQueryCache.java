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
package org.apache.lucene5_shaded.search;


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

import org.apache.lucene5_shaded.index.LeafReader.CoreClosedListener;
import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.index.ReaderUtil;
import org.apache.lucene5_shaded.index.Term;
import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.Accountables;
import org.apache.lucene5_shaded.util.RamUsageEstimator;
import org.apache.lucene5_shaded.util.RoaringDocIdSet;

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
 * Typical usage looks like this:
 * <pre class="prettyprint">
 *   final int maxNumberOfCachedQueries = 256;
 *   final long maxRamBytesUsed = 50 * 1024L * 1024L; // 50MB
 *   // these cache and policy instances can be shared across several queries and readers
 *   // it is fine to eg. store them into static variables
 *   final QueryCache queryCache = new LRUQueryCache(maxNumberOfCachedQueries, maxRamBytesUsed);
 *   final QueryCachingPolicy defaultCachingPolicy = new UsageTrackingQueryCachingPolicy();
 *
 *   // ...
 *
 *   // Then at search time
 *   Query myQuery = ...;
 *   Query myCacheQuery = queryCache.doCache(myQuery, defaultCachingPolicy);
 *   // myCacheQuery is now a wrapper around the original query that will interact with the cache
 *   IndexSearcher searcher = ...;
 *   TopDocs topDocs = searcher.search(new ConstantScoreQuery(myCacheQuery), 10);
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
 * @lucene.experimental
 */
public class LRUQueryCache implements QueryCache, Accountable {

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
  // maps queries that are contained in the cache to a singleton so that this
  // cache does not store several copies of the same query
  private final Map<Query, Query> uniqueQueries;
  // The contract between this set and the per-leaf caches is that per-leaf caches
  // are only allowed to store sub-sets of the queries that are contained in
  // mostRecentlyUsedQueries. This is why write operations are performed under a lock
  private final Set<Query> mostRecentlyUsedQueries;
  private final Map<Object, LeafCache> cache;

  // these variables are volatile so that we do not need to sync reads
  // but increments need to be performed under the lock
  private volatile long ramBytesUsed;
  private volatile long hitCount;
  private volatile long missCount;
  private volatile long cacheCount;
  private volatile long cacheSize;

  /**
   * Create a new instance that will cache at most <code>maxSize</code> queries
   * with at most <code>maxRamBytesUsed</code> bytes of memory.
   */
  public LRUQueryCache(int maxSize, long maxRamBytesUsed) {
    this.maxSize = maxSize;
    this.maxRamBytesUsed = maxRamBytesUsed;
    uniqueQueries = new LinkedHashMap<>(16, 0.75f, true);
    mostRecentlyUsedQueries = uniqueQueries.keySet();
    cache = new IdentityHashMap<>();
    ramBytesUsed = 0;
  }

  /**
   * Expert: callback when there is a cache hit on a given query.
   * Implementing this method is typically useful in order to compute more
   * fine-grained statistics about the query cache.
   * @see #onMiss
   * @lucene.experimental
   */
  protected void onHit(Object readerCoreKey, Query query) {
    hitCount += 1;
  }

  /**
   * Expert: callback when there is a cache miss on a given query.
   * @see #onHit
   * @lucene.experimental
   */
  protected void onMiss(Object readerCoreKey, Query query) {
    assert query != null;
    missCount += 1;
  }

  /**
   * Expert: callback when a query is added to this cache.
   * Implementing this method is typically useful in order to compute more
   * fine-grained statistics about the query cache.
   * @see #onQueryEviction
   * @lucene.experimental
   */
  protected void onQueryCache(Query query, long ramBytesUsed) {
    this.ramBytesUsed += ramBytesUsed;
  }

  /**
   * Expert: callback when a query is evicted from this cache.
   * @see #onQueryCache
   * @lucene.experimental
   */
  protected void onQueryEviction(Query query, long ramBytesUsed) {
    this.ramBytesUsed -= ramBytesUsed;
  }

  /**
   * Expert: callback when a {@link DocIdSet} is added to this cache.
   * Implementing this method is typically useful in order to compute more
   * fine-grained statistics about the query cache.
   * @see #onDocIdSetEviction
   * @lucene.experimental
   */
  protected void onDocIdSetCache(Object readerCoreKey, long ramBytesUsed) {
    cacheSize += 1;
    cacheCount += 1;
    this.ramBytesUsed += ramBytesUsed;
  }

  /**
   * Expert: callback when one or more {@link DocIdSet}s are removed from this
   * cache.
   * @see #onDocIdSetCache
   * @lucene.experimental
   */
  protected void onDocIdSetEviction(Object readerCoreKey, int numEntries, long sumRamBytesUsed) {
    this.ramBytesUsed -= sumRamBytesUsed;
    cacheSize -= numEntries;
  }

  /**
   * Expert: callback when the cache is completely cleared.
   * @lucene.experimental
   */
  protected void onClear() {
    ramBytesUsed = 0;
    cacheSize = 0;
  }

  /** Whether evictions are required. */
  boolean requiresEviction() {
    final int size = mostRecentlyUsedQueries.size();
    if (size == 0) {
      return false;
    } else {
      return size > maxSize || ramBytesUsed() > maxRamBytesUsed;
    }
  }

  synchronized DocIdSet get(Query key, LeafReaderContext context) {
    assert key.getBoost() == 1f;
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

  synchronized void putIfAbsent(Query query, LeafReaderContext context, DocIdSet set) {
    // under a lock to make sure that mostRecentlyUsedQueries and cache remain sync'ed
    // we don't want to have user-provided queries as keys in our cache since queries are mutable
    assert query instanceof BoostQuery == false;
    assert query instanceof ConstantScoreQuery == false;
    assert query.getBoost() == 1f;
    Query singleton = uniqueQueries.get(query);
    if (singleton == null) {
      uniqueQueries.put(query, query);
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
      context.reader().addCoreClosedListener(new CoreClosedListener() {
        @Override
        public void onClose(Object ownerCoreCacheKey) {
          clearCoreCacheKey(ownerCoreCacheKey);
        }
      });
    }
    leafCache.putIfAbsent(query, set);
    evictIfNecessary();
  }

  synchronized void evictIfNecessary() {
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
  public synchronized void clearCoreCacheKey(Object coreKey) {
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
  }

  /**
   * Remove all cache entries for the given query.
   */
  public synchronized void clearQuery(Query query) {
    final Query singleton = uniqueQueries.remove(query);
    if (singleton != null) {
      onEviction(singleton);
    }
  }

  private void onEviction(Query singleton) {
    onQueryEviction(singleton, LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY + ramBytesUsed(singleton));
    for (LeafCache leafCache : cache.values()) {
      leafCache.remove(singleton);
    }
  }

  /**
   * Clear the content of this cache.
   */
  public synchronized void clear() {
    cache.clear();
    mostRecentlyUsedQueries.clear();
    onClear();
  }

  // pkg-private for testing
  synchronized void assertConsistent() {
    if (requiresEviction()) {
      throw new AssertionError("requires evictions: size=" + mostRecentlyUsedQueries.size()
          + ", maxSize=" + maxSize + ", ramBytesUsed=" + ramBytesUsed() + ", maxRamBytesUsed=" + maxRamBytesUsed);
    }
    for (LeafCache leafCache : cache.values()) {
      Set<Query> keys = Collections.newSetFromMap(new IdentityHashMap<Query, Boolean>());
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
  }

  // pkg-private for testing
  // return the list of cached queries in LRU order
  synchronized List<Query> cachedQueries() {
    return new ArrayList<>(mostRecentlyUsedQueries);
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
    synchronized (this) {
      return Accountables.namedAccountables("segment", cache);
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
   * Default cache implementation: uses {@link RoaringDocIdSet}.
   */
  protected DocIdSet cacheImpl(BulkScorer scorer, int maxDoc) throws IOException {
    final RoaringDocIdSet.Builder builder = new RoaringDocIdSet.Builder(maxDoc);
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
      LRUQueryCache.this.onDocIdSetCache(key, ramBytesUsed);
    }

    private void onDocIdSetEviction(long ramBytesUsed) {
      this.ramBytesUsed -= ramBytesUsed;
      LRUQueryCache.this.onDocIdSetEviction(key, 1, ramBytesUsed);
    }

    DocIdSet get(Query query) {
      assert query instanceof BoostQuery == false;
      assert query instanceof ConstantScoreQuery == false;
      assert query.getBoost() == 1f;
      return cache.get(query);
    }

    void putIfAbsent(Query query, DocIdSet set) {
      assert query instanceof BoostQuery == false;
      assert query instanceof ConstantScoreQuery == false;
      assert query.getBoost() == 1f;
      if (cache.containsKey(query) == false) {
        cache.put(query, set);
        // the set was actually put
        onDocIdSetCache(HASHTABLE_RAM_BYTES_PER_ENTRY + set.ramBytesUsed());
      }
    }

    void remove(Query query) {
      assert query instanceof BoostQuery == false;
      assert query instanceof ConstantScoreQuery == false;
      assert query.getBoost() == 1f;
      DocIdSet removed = cache.remove(query);
      if (removed != null) {
        onDocIdSetEviction(HASHTABLE_RAM_BYTES_PER_ENTRY + removed.ramBytesUsed());
      }
    }

    @Override
    public long ramBytesUsed() {
      return ramBytesUsed;
    }

    @Override
    public Collection<Accountable> getChildResources() {
      return Collections.emptyList();
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

    private boolean shouldCache(LeafReaderContext context) throws IOException {
      return cacheEntryHasReasonableWorstCaseSize(ReaderUtil.getTopLevelContext(context).reader().maxDoc())
          && policy.shouldCache(in.getQuery(), context);
    }

    @Override
    public Scorer scorer(LeafReaderContext context) throws IOException {
      if (used.compareAndSet(false, true)) {
        policy.onUse(getQuery());
      }
      DocIdSet docIdSet = get(in.getQuery(), context);
      if (docIdSet == null) {
        if (shouldCache(context)) {
          docIdSet = cache(context);
          putIfAbsent(in.getQuery(), context, docIdSet);
        } else {
          return in.scorer(context);
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

      return new ConstantScoreScorer(this, 0f, disi);
    }

    @Override
    public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
      if (used.compareAndSet(false, true)) {
        policy.onUse(getQuery());
      }
      DocIdSet docIdSet = get(in.getQuery(), context);
      if (docIdSet == null) {
        if (shouldCache(context)) {
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
