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

import org.apache.lucene5_shaded.index.IndexReaderContext;
import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.index.ReaderUtil;
import org.apache.lucene5_shaded.index.TieredMergePolicy;

/**
 * A policy defining which filters should be cached.
 *
 * Implementations of this class must be thread-safe.
 *
 * @see UsageTrackingQueryCachingPolicy
 * @see LRUQueryCache
 * @lucene.experimental
 */
// TODO: add APIs for integration with IndexWriter.IndexReaderWarmer
public interface QueryCachingPolicy {

  /** A simple policy that caches all the provided filters on all segments. */
  public static final QueryCachingPolicy ALWAYS_CACHE = new QueryCachingPolicy() {

    @Override
    public void onUse(Query query) {}

    @Override
    public boolean shouldCache(Query query, LeafReaderContext context) throws IOException {
      return true;
    }

  };

  /** A simple policy that only caches on the largest segments of an index.
   *  The reasoning is that these segments likely account for most of the
   *  execution time of queries and are also more likely to stay around longer
   *  than small segments, which makes them more interesting for caching.
   */
  public static class CacheOnLargeSegments implements QueryCachingPolicy {

    /** {@link CacheOnLargeSegments} instance that only caches on segments that
     *  account for more than 3% of the total index size. This should guarantee
     *  that all segments from the upper {@link TieredMergePolicy tier} will be
     *  cached while ensuring that at most <tt>33</tt> segments can make it to
     *  the cache (given that some implementations such as {@link LRUQueryCache}
     *  perform better when the number of cached segments is low). */
    public static final CacheOnLargeSegments DEFAULT = new CacheOnLargeSegments(10000, 0.03f);

    private final int minIndexSize;
    private final float minSizeRatio;

    /**
     * Create a {@link CacheOnLargeSegments} instance that only caches on a
     * given segment if the total number of documents in the index is greater
     * than {@code minIndexSize} and the number of documents in the segment
     * divided by the total number of documents in the index is greater than
     * or equal to {@code minSizeRatio}.
     */
    public CacheOnLargeSegments(int minIndexSize, float minSizeRatio) {
      if (minSizeRatio <= 0 || minSizeRatio >= 1) {
        throw new IllegalArgumentException("minSizeRatio must be in ]0, 1[, got " + minSizeRatio);
      }
      this.minIndexSize = minIndexSize;
      this.minSizeRatio = minSizeRatio;
    }

    @Override
    public void onUse(Query query) {}

    @Override
    public boolean shouldCache(Query query, LeafReaderContext context) throws IOException {
      final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(context);
      if (topLevelContext.reader().maxDoc() < minIndexSize) {
        return false;
      }
      final float sizeRatio = (float) context.reader().maxDoc() / topLevelContext.reader().maxDoc();
      return sizeRatio >= minSizeRatio;
    }

  };

  /** Callback that is called every time that a cached filter is used.
   *  This is typically useful if the policy wants to track usage statistics
   *  in order to make decisions. */
  void onUse(Query query);

  /** Whether the given {@link DocIdSet} should be cached on a given segment.
   *  This method will be called on each leaf context to know if the filter
   *  should be cached on this particular leaf. The filter cache will first
   *  attempt to load a {@link DocIdSet} from the cache. If it is not cached
   *  yet and this method returns <tt>true</tt> then a cache entry will be
   *  generated. Otherwise an uncached set will be returned. */
  boolean shouldCache(Query query, LeafReaderContext context) throws IOException;

}
