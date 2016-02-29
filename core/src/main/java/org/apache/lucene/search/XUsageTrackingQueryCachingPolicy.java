// DO NOT EDIT
// Copied from Lucene commit fef99d932d2accced7e2f72be2417a5669d702f6

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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.queries.TermsQuery;
import org.apache.lucene.util.FrequencyTrackingRingBuffer;


/**
 * A {@link QueryCachingPolicy} that tracks usage statistics of recently-used
 * filters in order to decide on which filters are worth caching.
 *
 * It also uses some heuristics on segments, filters and the doc id sets that
 * they produce in order to cache more aggressively when the execution cost
 * significantly outweighs the caching overhead.
 *
 * @lucene.experimental
 */
public final class XUsageTrackingQueryCachingPolicy implements QueryCachingPolicy {

  // the hash code that we use as a sentinel in the ring buffer.
  private static final int SENTINEL = Integer.MIN_VALUE;

  static boolean isCostly(Query query) {
    // This does not measure the cost of iterating over the filter (for this we
    // already have the DocIdSetIterator#cost API) but the cost to build the
    // DocIdSet in the first place
    // ========== BEGIN EDIT ==========
    /*return query instanceof MultiTermQuery ||
        query instanceof MultiTermQueryConstantScoreWrapper;*/
    return query instanceof MultiTermQuery ||
        query instanceof MultiTermQueryConstantScoreWrapper ||
        query instanceof TermsQuery;
    // ========== END EDIT ==========
  }

  static boolean isCheap(Query query) {
    // same for cheap queries
    // these queries are so cheap that they usually do not need caching
    return query instanceof TermQuery;
  }

  private final QueryCachingPolicy.CacheOnLargeSegments segmentPolicy;
  private final FrequencyTrackingRingBuffer recentlyUsedFilters;

  /**
   * Create a new instance.
   *
   * @param minIndexSize              the minimum size of the top-level index
   * @param minSizeRatio              the minimum size ratio for segments to be cached, see {@link QueryCachingPolicy.CacheOnLargeSegments}
   * @param historySize               the number of recently used filters to track
   */
  public XUsageTrackingQueryCachingPolicy(
      int minIndexSize,
      float minSizeRatio,
      int historySize) {
    this(new QueryCachingPolicy.CacheOnLargeSegments(minIndexSize, minSizeRatio), historySize);
  }

  /** Create a new instance with an history size of 256. */
  public XUsageTrackingQueryCachingPolicy() {
    this(QueryCachingPolicy.CacheOnLargeSegments.DEFAULT, 256);
  }

  private XUsageTrackingQueryCachingPolicy(
      QueryCachingPolicy.CacheOnLargeSegments segmentPolicy,
      int historySize) {
    this.segmentPolicy = segmentPolicy;
    this.recentlyUsedFilters = new FrequencyTrackingRingBuffer(historySize, SENTINEL);
  }

  /**
   * For a given query, return how many times it should appear in the history
   * before being cached.
   */
  protected int minFrequencyToCache(Query query) {
    if (isCostly(query)) {
      return 2;
    } else if (isCheap(query)) {
      return 20;
    } else {
      // default: cache after the filter has been seen 5 times
      return 5;
    }
  }

  @Override
  public void onUse(Query query) {
    assert query instanceof BoostQuery == false;
    assert query instanceof ConstantScoreQuery == false;

    // call hashCode outside of sync block
    // in case it's somewhat expensive:
    int hashCode = query.hashCode();

    // we only track hash codes to avoid holding references to possible
    // large queries; this may cause rare false positives, but at worse
    // this just means we cache a query that was not in fact used enough:
    synchronized (this) {
      recentlyUsedFilters.add(hashCode);
    }
  }

  int frequency(Query query) {
    assert query instanceof BoostQuery == false;
    assert query instanceof ConstantScoreQuery == false;

    // call hashCode outside of sync block
    // in case it's somewhat expensive:
    int hashCode = query.hashCode();

    synchronized (this) {
      return recentlyUsedFilters.frequency(hashCode);
    }
  }

  @Override
  public boolean shouldCache(Query query, LeafReaderContext context) throws IOException {
    if (query instanceof MatchAllDocsQuery
        // MatchNoDocsQuery currently rewrites to a BooleanQuery,
        // but who knows, it might get its own Weight one day
        || query instanceof MatchNoDocsQuery) {
      return false;
    }
    if (query instanceof BooleanQuery) {
      BooleanQuery bq = (BooleanQuery) query;
      if (bq.clauses().isEmpty()) {
        return false;
      }
    }
    if (query instanceof DisjunctionMaxQuery) {
      DisjunctionMaxQuery dmq = (DisjunctionMaxQuery) query;
      if (dmq.getDisjuncts().isEmpty()) {
        return false;
      }
    }
    if (segmentPolicy.shouldCache(query, context) == false) {
      return false;
    }
    final int frequency = frequency(query);
    final int minFrequency = minFrequencyToCache(query);
    return frequency >= minFrequency;
  }

}
