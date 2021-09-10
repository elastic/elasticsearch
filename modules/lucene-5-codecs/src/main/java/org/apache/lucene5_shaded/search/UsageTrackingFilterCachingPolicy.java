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

import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.FrequencyTrackingRingBuffer;


/**
 * A {@link FilterCachingPolicy} that tracks usage statistics of recently-used
 * filters in order to decide on which filters are worth caching.
 *
 * It also uses some heuristics on segments, filters and the doc id sets that
 * they produce in order to cache more aggressively when the execution cost
 * significantly outweighs the caching overhead.
 *
 * @lucene.experimental
 */
public final class UsageTrackingFilterCachingPolicy implements FilterCachingPolicy {

  // the hash code that we use as a sentinel in the ring buffer.
  private static final int SENTINEL = Integer.MIN_VALUE;

  static boolean isCostly(Filter filter) {
    // This does not measure the cost of iterating over the filter (for this we
    // already have the DocIdSetIterator#cost API) but the cost to build the
    // DocIdSet in the first place
    return filter instanceof QueryWrapperFilter && ((QueryWrapperFilter) filter).getQuery() instanceof MultiTermQuery;
  }

  static boolean isCheapToCache(DocIdSet set) {
    // the produced doc set is already cacheable, so caching has no
    // overhead at all. TODO: extend this to sets whose iterators have a low
    // cost?
    return set == null || set.isCacheable();
  }

  private final CacheOnLargeSegments segmentPolicy;
  private final FrequencyTrackingRingBuffer recentlyUsedFilters;
  private final int minFrequencyCostlyFilters;
  private final int minFrequencyCheapFilters;
  private final int minFrequencyOtherFilters;

  /**
   * Create a new instance.
   *
   * @param minSizeRatio              the minimum size ratio for segments to be cached, see {@link CacheOnLargeSegments}
   * @param historySize               the number of recently used filters to track
   * @param minFrequencyCostlyFilters how many times filters whose {@link Filter#getDocIdSet(LeafReaderContext, Bits) getDocIdSet} method is expensive should have been seen before being cached
   * @param minFrequencyCheapFilters  how many times filters that produce {@link DocIdSet}s that are cheap to cached should have been seen before being cached
   * @param minFrequencyOtherFilters  how many times other filters should have been seen before being cached
   */
  public UsageTrackingFilterCachingPolicy(
      float minSizeRatio,
      int historySize,
      int minFrequencyCostlyFilters,
      int minFrequencyCheapFilters,
      int minFrequencyOtherFilters) {
    this(new CacheOnLargeSegments(minSizeRatio), historySize,
        minFrequencyCostlyFilters, minFrequencyCheapFilters, minFrequencyOtherFilters);
  }

  /** Create a new instance with sensible defaults. */
  public UsageTrackingFilterCachingPolicy() {
    // we track the most 256 recently-used filters and cache filters that are
    // expensive to build or cheap to cache after we have seen them twice, and
    // cache regular filters after we have seen them 5 times
    this(CacheOnLargeSegments.DEFAULT, 256, 2, 2, 5);
  }

  private UsageTrackingFilterCachingPolicy(
      CacheOnLargeSegments segmentPolicy,
      int historySize,
      int minFrequencyCostlyFilters,
      int minFrequencyCheapFilters,
      int minFrequencyOtherFilters) {
    this.segmentPolicy = segmentPolicy;
    if (minFrequencyOtherFilters < minFrequencyCheapFilters || minFrequencyOtherFilters < minFrequencyCheapFilters) {
      throw new IllegalArgumentException("it does not make sense to cache regular filters more aggressively than filters that are costly to produce or cheap to cache");
    }
    if (minFrequencyCheapFilters > historySize || minFrequencyCostlyFilters > historySize || minFrequencyOtherFilters > historySize) {
      throw new IllegalArgumentException("The minimum frequencies should be less than the size of the history of filters that are being tracked");
    }
    this.recentlyUsedFilters = new FrequencyTrackingRingBuffer(historySize, SENTINEL);
    this.minFrequencyCostlyFilters = minFrequencyCostlyFilters;
    this.minFrequencyCheapFilters = minFrequencyCheapFilters;
    this.minFrequencyOtherFilters = minFrequencyOtherFilters;
  }

  @Override
  public void onUse(Filter filter) {
    // we only track hash codes, which
    synchronized (this) {
      recentlyUsedFilters.add(filter.hashCode());
    }
  }

  @Override
  public boolean shouldCache(Filter filter, LeafReaderContext context, DocIdSet set) throws IOException {
    if (segmentPolicy.shouldCache(filter, context, set) == false) {
      return false;
    }
    final int frequency;
    synchronized (this) {
      frequency = recentlyUsedFilters.frequency(filter.hashCode());
    }
    if (frequency >= minFrequencyOtherFilters) {
      return true;
    } else if (isCostly(filter) && frequency >= minFrequencyCostlyFilters) {
      return true;
    } else if (isCheapToCache(set) && frequency >= minFrequencyCheapFilters) {
      return true;
    }

    return false;
  }

}
