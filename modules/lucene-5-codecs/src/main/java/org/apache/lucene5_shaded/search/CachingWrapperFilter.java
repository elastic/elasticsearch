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

import static org.apache.lucene5_shaded.search.DocIdSet.EMPTY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.lucene5_shaded.index.LeafReader;
import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.Accountables;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.RoaringDocIdSet;

/**
 * Wraps another {@link Filter}'s result and caches it.  The purpose is to allow
 * filters to simply filter, and then wrap with this class
 * to add caching.
 * @deprecated Use {@link CachingWrapperQuery} and {@link BooleanClause.Occur#FILTER} clauses instead
 */
@Deprecated
public class CachingWrapperFilter extends Filter implements Accountable {
  private final Filter filter;
  private final FilterCachingPolicy policy;
  private final Map<Object,DocIdSet> cache = Collections.synchronizedMap(new WeakHashMap<Object,DocIdSet>());

  /** Wraps another filter's result and caches it according to the provided policy.
   * @param filter Filter to cache results of
   * @param policy policy defining which filters should be cached on which segments
   */
  public CachingWrapperFilter(Filter filter, FilterCachingPolicy policy) {
    this.filter = filter;
    this.policy = policy;
  }

  /** Same as {@link CachingWrapperFilter#CachingWrapperFilter(Filter, FilterCachingPolicy)}
   *  but enforces the use of the
   *  {@link FilterCachingPolicy.CacheOnLargeSegments#DEFAULT} policy. */
  public CachingWrapperFilter(Filter filter) {
    this(filter, FilterCachingPolicy.CacheOnLargeSegments.DEFAULT);
  }

  /**
   * Gets the contained filter.
   * @return the contained filter.
   */
  public Filter getFilter() {
    return filter;
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
  
  /**
   * Default cache implementation: uses {@link RoaringDocIdSet}.
   */
  protected DocIdSet cacheImpl(DocIdSetIterator iterator, LeafReader reader) throws IOException {
    return new RoaringDocIdSet.Builder(reader.maxDoc()).add(iterator).build();
  }

  // for testing
  int hitCount, missCount;

  @Override
  public DocIdSet getDocIdSet(LeafReaderContext context, final Bits acceptDocs) throws IOException {
    final LeafReader reader = context.reader();
    final Object key = reader.getCoreCacheKey();

    DocIdSet docIdSet = cache.get(key);
    if (docIdSet != null) {
      hitCount++;
    } else {
      docIdSet = filter.getDocIdSet(context, null);
      if (policy.shouldCache(filter, context, docIdSet)) {
        missCount++;
        docIdSet = docIdSetToCache(docIdSet, reader);
        if (docIdSet == null) {
          // We use EMPTY as a sentinel for the empty set, which is cacheable
          docIdSet = EMPTY;
        }
        assert docIdSet.isCacheable();
        cache.put(key, docIdSet);
      }
    }

    return docIdSet == EMPTY ? null : BitsFilteredDocIdSet.wrap(docIdSet, acceptDocs);
  }
  
  @Override
  public String toString(String field) {
    return getClass().getSimpleName() + "("+filter.toString(field)+")";
  }

  @Override
  public boolean equals(Object o) {
    if (super.equals(o) == false) {
      return false;
    }
    final CachingWrapperFilter other = (CachingWrapperFilter) o;
    return this.filter.equals(other.filter);
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + filter.hashCode();
  }

  @Override
  public long ramBytesUsed() {

    // Sync only to pull the current set of values:
    List<DocIdSet> docIdSets;
    synchronized(cache) {
      docIdSets = new ArrayList<>(cache.values());
    }

    long total = 0;
    for(DocIdSet dis : docIdSets) {
      total += dis.ramBytesUsed();
    }

    return total;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    // Sync to pull the current set of values:
    synchronized (cache) {
      // no need to clone, Accountable#namedAccountables already copies the data
      return Accountables.namedAccountables("segment", cache);
    }
  }
}
