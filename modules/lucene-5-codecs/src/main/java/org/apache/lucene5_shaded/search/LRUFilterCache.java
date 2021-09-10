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
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene5_shaded.index.LeafReader;
import org.apache.lucene5_shaded.index.LeafReader.CoreClosedListener;
import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.Accountables;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.RamUsageEstimator;
import org.apache.lucene5_shaded.util.RoaringDocIdSet;

/**
 * A {@link FilterCache} that evicts filters using a LRU (least-recently-used)
 * eviction policy in order to remain under a given maximum size and number of
 * bytes used.
 *
 * This class is thread-safe.
 *
 * Note that filter eviction runs in linear time with the total number of
 * segments that have cache entries so this cache works best with
 * {@link FilterCachingPolicy caching policies} that only cache on "large"
 * segments, and it is advised to not share this cache across too many indices.
 *
 * Typical usage looks like this:
 * <pre class="prettyprint">
 *   final int maxNumberOfCachedFilters = 256;
 *   final long maxRamBytesUsed = 50 * 1024L * 1024L; // 50MB
 *   // these cache and policy instances can be shared across several filters and readers
 *   // it is fine to eg. store them into static variables
 *   final FilterCache filterCache = new LRUFilterCache(maxNumberOfCachedFilters, maxRamBytesUsed);
 *   final FilterCachingPolicy defaultCachingPolicy = new UsageTrackingFilterCachingPolicy();
 *   
 *   // ...
 *   
 *   // Then at search time
 *   Filter myFilter = ...;
 *   Filter myCacheFilter = filterCache.doCache(myFilter, defaultCachingPolicy);
 *   // myCacheFilter is now a wrapper around the original filter that will interact with the cache
 *   IndexSearcher searcher = ...;
 *   TopDocs topDocs = searcher.search(new ConstantScoreQuery(myCacheFilter), 10);
 * </pre>
 *
 * This cache exposes some global statistics ({@link #getHitCount() hit count},
 * {@link #getMissCount() miss count}, {@link #getCacheSize() number of cache
 * entries}, {@link #getCacheCount() total number of DocIdSets that have ever
 * been cached}, {@link #getEvictionCount() number of evicted entries}). In
 * case you would like to have more fine-grained statistics, such as per-index
 * or per-filter-class statistics, it is possible to override various callbacks:
 * {@link #onHit}, {@link #onMiss},
 * {@link #onFilterCache}, {@link #onFilterEviction},
 * {@link #onDocIdSetCache}, {@link #onDocIdSetEviction} and {@link #onClear}.
 * It is better to not perform heavy computations in these methods though since
 * they are called synchronously and under a lock.
 *
 * @see FilterCachingPolicy
 * @lucene.experimental
 * @deprecated Use {@link LRUQueryCache} instead
 */
@Deprecated
public class LRUFilterCache implements FilterCache, Accountable {

  // memory usage of a simple query-wrapper filter around a term query
  static final long FILTER_DEFAULT_RAM_BYTES_USED = 216;

  static final long HASHTABLE_RAM_BYTES_PER_ENTRY =
      2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF // key + value
      * 2; // hash tables need to be oversized to avoid collisions, assume 2x capacity

  static final long LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY =
      HASHTABLE_RAM_BYTES_PER_ENTRY
      + 2 * RamUsageEstimator.NUM_BYTES_OBJECT_REF; // previous & next references

  private final int maxSize;
  private final long maxRamBytesUsed;
  // maps filters that are contained in the cache to a singleton so that this
  // cache does not store several copies of the same filter
  private final Map<Filter, Filter> uniqueFilters;
  // The contract between this set and the per-leaf caches is that per-leaf caches
  // are only allowed to store sub-sets of the filters that are contained in
  // mostRecentlyUsedFilters. This is why write operations are performed under a lock
  private final Set<Filter> mostRecentlyUsedFilters;
  private final Map<Object, LeafCache> cache;

  // these variables are volatile so that we do not need to sync reads
  // but increments need to be performed under the lock
  private volatile long ramBytesUsed;
  private volatile long hitCount;
  private volatile long missCount;
  private volatile long cacheCount;
  private volatile long cacheSize;

  /**
   * Create a new instance that will cache at most <code>maxSize</code> filters
   * with at most <code>maxRamBytesUsed</code> bytes of memory.
   */
  public LRUFilterCache(int maxSize, long maxRamBytesUsed) {
    this.maxSize = maxSize;
    this.maxRamBytesUsed = maxRamBytesUsed;
    uniqueFilters = new LinkedHashMap<Filter, Filter>(16, 0.75f, true);
    mostRecentlyUsedFilters = uniqueFilters.keySet();
    cache = new IdentityHashMap<>();
    ramBytesUsed = 0;
  }

  /**
   * Expert: callback when there is a cache hit on a given filter.
   * Implementing this method is typically useful in order to compute more
   * fine-grained statistics about the filter cache.
   * @see #onMiss
   * @lucene.experimental
   */
  protected void onHit(Object readerCoreKey, Filter filter) {
    hitCount += 1;
  }

  /**
   * Expert: callback when there is a cache miss on a given filter.
   * @see #onHit
   * @lucene.experimental
   */
  protected void onMiss(Object readerCoreKey, Filter filter) {
    assert filter != null;
    missCount += 1;
  }

  /**
   * Expert: callback when a filter is added to this cache.
   * Implementing this method is typically useful in order to compute more
   * fine-grained statistics about the filter cache.
   * @see #onFilterEviction
   * @lucene.experimental
   */
  protected void onFilterCache(Filter filter, long ramBytesUsed) {
    this.ramBytesUsed += ramBytesUsed;
  }

  /**
   * Expert: callback when a filter is evicted from this cache.
   * @see #onFilterCache
   * @lucene.experimental
   */
  protected void onFilterEviction(Filter filter, long ramBytesUsed) {
    this.ramBytesUsed -= ramBytesUsed;
  }

  /**
   * Expert: callback when a {@link DocIdSet} is added to this cache.
   * Implementing this method is typically useful in order to compute more
   * fine-grained statistics about the filter cache.
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
    final int size = mostRecentlyUsedFilters.size();
    if (size == 0) {
      return false;
    } else {
      return size > maxSize || ramBytesUsed() > maxRamBytesUsed;
    }
  }

  synchronized DocIdSet get(Filter filter, LeafReaderContext context) {
    final Object readerKey = context.reader().getCoreCacheKey();
    final LeafCache leafCache = cache.get(readerKey);
    if (leafCache == null) {
      onMiss(readerKey, filter);
      return null;
    }
    // this get call moves the filter to the most-recently-used position
    final Filter singleton = uniqueFilters.get(filter);
    if (singleton == null) {
      onMiss(readerKey, filter);
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

  synchronized void putIfAbsent(Filter filter, LeafReaderContext context, DocIdSet set) {
    // under a lock to make sure that mostRecentlyUsedFilters and cache remain sync'ed
    assert set.isCacheable();
    Filter singleton = uniqueFilters.get(filter);
    if (singleton == null) {
      uniqueFilters.put(filter, filter);
      onFilterCache(singleton, LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY + ramBytesUsed(filter));
    } else {
      filter = singleton;
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
    leafCache.putIfAbsent(filter, set);
    evictIfNecessary();
  }

  synchronized void evictIfNecessary() {
    // under a lock to make sure that mostRecentlyUsedFilters and cache keep sync'ed
    if (requiresEviction()) {
      Iterator<Filter> iterator = mostRecentlyUsedFilters.iterator();
      do {
        final Filter filter = iterator.next();
        iterator.remove();
        onEviction(filter);
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
      onDocIdSetEviction(coreKey, leafCache.cache.size(), leafCache.ramBytesUsed);
    }
  }

  /**
   * Remove all cache entries for the given filter.
   */
  public synchronized void clearFilter(Filter filter) {
    final Filter singleton = uniqueFilters.remove(filter);
    if (singleton != null) {
      onEviction(singleton);
    }
  }

  private void onEviction(Filter singleton) {
    onFilterEviction(singleton, LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY + ramBytesUsed(singleton));
    for (LeafCache leafCache : cache.values()) {
      leafCache.remove(singleton);
    }
  }

  /**
   * Clear the content of this cache.
   */
  public synchronized void clear() {
    cache.clear();
    mostRecentlyUsedFilters.clear();
    onClear();
  }

  // pkg-private for testing
  synchronized void assertConsistent() {
    if (requiresEviction()) {
      throw new AssertionError("requires evictions: size=" + mostRecentlyUsedFilters.size()
          + ", maxSize=" + maxSize + ", ramBytesUsed=" + ramBytesUsed() + ", maxRamBytesUsed=" + maxRamBytesUsed);
    }
    for (LeafCache leafCache : cache.values()) {
      Set<Filter> keys = Collections.newSetFromMap(new IdentityHashMap<Filter, Boolean>());
      keys.addAll(leafCache.cache.keySet());
      keys.removeAll(mostRecentlyUsedFilters);
      if (!keys.isEmpty()) {
        throw new AssertionError("One leaf cache contains more keys than the top-level cache: " + keys);
      }
    }
    long recomputedRamBytesUsed =
          HASHTABLE_RAM_BYTES_PER_ENTRY * cache.size()
        + LINKED_HASHTABLE_RAM_BYTES_PER_ENTRY * uniqueFilters.size();
    for (Filter filter : mostRecentlyUsedFilters) {
      recomputedRamBytesUsed += ramBytesUsed(filter);
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
  // return the list of cached filters in LRU order
  synchronized List<Filter> cachedFilters() {
    return new ArrayList<>(mostRecentlyUsedFilters);
  }

  @Override
  public Filter doCache(Filter filter, FilterCachingPolicy policy) {
    while (filter instanceof CachingWrapperFilter) {
      // should we throw an exception instead?
      filter = ((CachingWrapperFilter) filter).in;
    }

    return new CachingWrapperFilter(filter, policy);
  }

  /**
   *  Provide the DocIdSet to be cached, using the DocIdSet provided
   *  by the wrapped Filter. <p>This implementation returns the given {@link DocIdSet},
   *  if {@link DocIdSet#isCacheable} returns <code>true</code>, else it calls
   *  {@link #cacheImpl(DocIdSetIterator, LeafReader)}
   *  <p>Note: This method returns {@linkplain DocIdSet#EMPTY} if the given docIdSet
   *  is <code>null</code> or if {@link DocIdSet#iterator()} return <code>null</code>. The empty
   *  instance is use as a placeholder in the cache instead of the <code>null</code> value.
   */
  protected DocIdSet docIdSetToCache(DocIdSet docIdSet, LeafReader reader) throws IOException {
    if (docIdSet == null || docIdSet.isCacheable()) {
      return docIdSet;
    } else {
      final DocIdSetIterator it = docIdSet.iterator();
      if (it == null) {
        return null;
      } else {
        return cacheImpl(it, reader);
      }
    }
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
   * Return the number of bytes used by the given filter. The default
   * implementation returns {@link Accountable#ramBytesUsed()} if the filter
   * implements {@link Accountable} and <code>1024</code> otherwise.
   */
  protected long ramBytesUsed(Filter filter) {
    if (filter instanceof Accountable) {
      return ((Accountable) filter).ramBytesUsed();
    }
    return FILTER_DEFAULT_RAM_BYTES_USED;
  }

  /**
   * Default cache implementation: uses {@link RoaringDocIdSet}.
   */
  protected DocIdSet cacheImpl(DocIdSetIterator iterator, LeafReader reader) throws IOException {
    return new RoaringDocIdSet.Builder(reader.maxDoc()).add(iterator).build();
  }

  /**
   * Return the total number of times that a {@link Filter} has been looked up
   * in this {@link FilterCache}. Note that this number is incremented once per
   * segment so running a cached filter only once will increment this counter
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
   * Over the {@link #getTotalCount() total} number of times that a filter has
   * been looked up, return how many times a cached {@link DocIdSet} has been
   * found and returned.
   * @see #getTotalCount()
   * @see #getMissCount()
   */
  public final long getHitCount() {
    return hitCount;
  }

  /**
   * Over the {@link #getTotalCount() total} number of times that a filter has
   * been looked up, return how many times this filter was not contained in the
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
   * as the opposite would indicate that the filter cache makes efforts in order
   * to cache filters but then they do not get reused.
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
   * that filters are not reused or that the {@link FilterCachingPolicy
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
    private final Map<Filter, DocIdSet> cache;
    private volatile long ramBytesUsed;

    LeafCache(Object key) {
      this.key = key;
      cache = new IdentityHashMap<>();
      ramBytesUsed = 0;
    }

    private void onDocIdSetCache(long ramBytesUsed) {
      this.ramBytesUsed += ramBytesUsed;
      LRUFilterCache.this.onDocIdSetCache(key, ramBytesUsed);
    }

    private void onDocIdSetEviction(long ramBytesUsed) {
      this.ramBytesUsed -= ramBytesUsed;
      LRUFilterCache.this.onDocIdSetEviction(key, 1, ramBytesUsed);
    }

    DocIdSet get(Filter filter) {
      return cache.get(filter);
    }

    void putIfAbsent(Filter filter, DocIdSet set) {
      if (cache.containsKey(filter) == false) {
        cache.put(filter, set);
        onDocIdSetCache(HASHTABLE_RAM_BYTES_PER_ENTRY + set.ramBytesUsed());
      }
    }

    void remove(Filter filter) {
      DocIdSet removed = cache.remove(filter);
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

  private class CachingWrapperFilter extends Filter {

    private final Filter in;
    private final FilterCachingPolicy policy;

    CachingWrapperFilter(Filter in, FilterCachingPolicy policy) {
      this.in = in;
      this.policy = policy;
    }

    @Override
    public DocIdSet getDocIdSet(LeafReaderContext context, Bits acceptDocs) throws IOException {
      if (context.ord == 0) {
        policy.onUse(in);
      }

      DocIdSet set = get(in, context);
      if (set == null) {
        // do not apply acceptDocs yet, we want the cached filter to not take them into account
        set = in.getDocIdSet(context, null);
        if (policy.shouldCache(in, context, set)) {
          set = docIdSetToCache(set, context.reader());
          if (set == null) {
            // null values are not supported
            set = DocIdSet.EMPTY;
          }
          // it might happen that another thread computed the same set in parallel
          // although this might incur some CPU overhead, it is probably better
          // this way than trying to lock and preventing other filters to be
          // computed at the same time?
          putIfAbsent(in, context, set);
        }
      }
      return set == DocIdSet.EMPTY ? null : BitsFilteredDocIdSet.wrap(set, acceptDocs);
    }

    @Override
    public boolean equals(Object obj) {
      return super.equals(obj)
          && in.equals(((CachingWrapperFilter) obj).in);
    }

    @Override
    public int hashCode() {
      return 31 * super.hashCode() + in.hashCode();
    }

    @Override
    public String toString(String field) {
      return "CachingWrapperFilter(" + in.toString(field) + ")";
    }
  }

}
