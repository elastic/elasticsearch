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
import java.util.Arrays;
import java.util.List;

import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.util.ArrayUtil;
import org.apache.lucene5_shaded.util.RamUsageEstimator;

/**
 * Caches all docs, and optionally also scores, coming from
 * a search, and is then able to replay them to another
 * collector.  You specify the max RAM this class may use.
 * Once the collection is done, call {@link #isCached}. If
 * this returns true, you can use {@link #replay(Collector)}
 * against a new collector.  If it returns false, this means
 * too much RAM was required and you must instead re-run the
 * original search.
 *
 * <p><b>NOTE</b>: this class consumes 4 (or 8 bytes, if
 * scoring is cached) per collected document.  If the result
 * set is large this can easily be a very substantial amount
 * of RAM!
 *
 * <p>See the Lucene <tt>modules/grouping</tt> module for more
 * details including a full code example.</p>
 *
 * @lucene.experimental
 */
public abstract class CachingCollector extends FilterCollector {

  private static final int INITIAL_ARRAY_SIZE = 128;

  private static final class CachedScorer extends Scorer {

    // NOTE: these members are package-private b/c that way accessing them from
    // the outer class does not incur access check by the JVM. The same
    // situation would be if they were defined in the outer class as private
    // members.
    int doc;
    float score;

    private CachedScorer() { super(null); }

    @Override
    public DocIdSetIterator iterator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public final float score() { return score; }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public final int freq() { throw new UnsupportedOperationException(); }

  }

  private static class NoScoreCachingCollector extends CachingCollector {

    List<LeafReaderContext> contexts;
    List<int[]> docs;
    int maxDocsToCache;
    NoScoreCachingLeafCollector lastCollector;

    NoScoreCachingCollector(Collector in, int maxDocsToCache) {
      super(in);
      this.maxDocsToCache = maxDocsToCache;
      contexts = new ArrayList<>();
      docs = new ArrayList<>();
    }

    protected NoScoreCachingLeafCollector wrap(LeafCollector in, int maxDocsToCache) {
      return new NoScoreCachingLeafCollector(in, maxDocsToCache);
    }

    // note: do *not* override needScore to say false. Just because we aren't caching the score doesn't mean the
    //   wrapped collector doesn't need it to do its job.

    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      postCollection();
      final LeafCollector in = this.in.getLeafCollector(context);
      if (contexts != null) {
        contexts.add(context);
      }
      if (maxDocsToCache >= 0) {
        return lastCollector = wrap(in, maxDocsToCache);
      } else {
        return in;
      }
    }

    protected void invalidate() {
      maxDocsToCache = -1;
      contexts = null;
      this.docs = null;
    }

    protected void postCollect(NoScoreCachingLeafCollector collector) {
      final int[] docs = collector.cachedDocs();
      maxDocsToCache -= docs.length;
      this.docs.add(docs);
    }

    private void postCollection() {
      if (lastCollector != null) {
        if (!lastCollector.hasCache()) {
          invalidate();
        } else {
          postCollect(lastCollector);
        }
        lastCollector = null;
      }
    }

    protected void collect(LeafCollector collector, int i) throws IOException {
      final int[] docs = this.docs.get(i);
      for (int doc : docs) {
        collector.collect(doc);
      }
    }

    public void replay(Collector other) throws IOException {
      postCollection();
      if (!isCached()) {
        throw new IllegalStateException("cannot replay: cache was cleared because too much RAM was required");
      }
      assert docs.size() == contexts.size();
      for (int i = 0; i < contexts.size(); ++i) {
        final LeafReaderContext context = contexts.get(i);
        final LeafCollector collector = other.getLeafCollector(context);
        collect(collector, i);
      }
    }

  }

  private static class ScoreCachingCollector extends NoScoreCachingCollector {

    List<float[]> scores;

    ScoreCachingCollector(Collector in, int maxDocsToCache) {
      super(in, maxDocsToCache);
      scores = new ArrayList<>();
    }

    protected NoScoreCachingLeafCollector wrap(LeafCollector in, int maxDocsToCache) {
      return new ScoreCachingLeafCollector(in, maxDocsToCache);
    }

    @Override
    protected void postCollect(NoScoreCachingLeafCollector collector) {
      final ScoreCachingLeafCollector coll = (ScoreCachingLeafCollector) collector;
      super.postCollect(coll);
      scores.add(coll.cachedScores());
    }

    /** Ensure the scores are collected so they can be replayed, even if the wrapped collector doesn't need them. */
    @Override
    public boolean needsScores() {
      return true;
    }

    @Override
    protected void collect(LeafCollector collector, int i) throws IOException {
      final int[] docs = this.docs.get(i);
      final float[] scores = this.scores.get(i);
      assert docs.length == scores.length;
      final CachedScorer scorer = new CachedScorer();
      collector.setScorer(scorer);
      for (int j = 0; j < docs.length; ++j) {
        scorer.doc = docs[j];
        scorer.score = scores[j];
        collector.collect(scorer.doc);
      }
    }
  }

  private class NoScoreCachingLeafCollector extends FilterLeafCollector {

    final int maxDocsToCache;
    int[] docs;
    int docCount;

    NoScoreCachingLeafCollector(LeafCollector in, int maxDocsToCache) {
      super(in);
      this.maxDocsToCache = maxDocsToCache;
      docs = new int[Math.min(maxDocsToCache, INITIAL_ARRAY_SIZE)];
      docCount = 0;
    }

    protected void grow(int newLen) {
      docs = Arrays.copyOf(docs, newLen);
    }

    protected void invalidate() {
      docs = null;
      docCount = -1;
      cached = false;
    }

    protected void buffer(int doc) throws IOException {
      docs[docCount] = doc;
    }

    @Override
    public void collect(int doc) throws IOException {
      if (docs != null) {
        if (docCount >= docs.length) {
          if (docCount >= maxDocsToCache) {
            invalidate();
          } else {
            final int newLen = Math.min(ArrayUtil.oversize(docCount + 1, RamUsageEstimator.NUM_BYTES_INT), maxDocsToCache);
            grow(newLen);
          }
        }
        if (docs != null) {
          buffer(doc);
          ++docCount;
        }
      }
      super.collect(doc);
    }

    boolean hasCache() {
      return docs != null;
    }

    int[] cachedDocs() {
      return docs == null ? null : Arrays.copyOf(docs, docCount);
    }

  }

  private class ScoreCachingLeafCollector extends NoScoreCachingLeafCollector {

    Scorer scorer;
    float[] scores;

    ScoreCachingLeafCollector(LeafCollector in, int maxDocsToCache) {
      super(in, maxDocsToCache);
      scores = new float[docs.length];
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
      super.setScorer(scorer);
    }

    @Override
    protected void grow(int newLen) {
      super.grow(newLen);
      scores = Arrays.copyOf(scores, newLen);
    }

    @Override
    protected void invalidate() {
      super.invalidate();
      scores = null;
    }

    @Override
    protected void buffer(int doc) throws IOException {
      super.buffer(doc);
      scores[docCount] = scorer.score();
    }

    float[] cachedScores() {
      return docs == null ? null : Arrays.copyOf(scores, docCount);
    }
  }

  /**
   * Creates a {@link CachingCollector} which does not wrap another collector.
   * The cached documents and scores can later be {@link #replay(Collector)
   * replayed}.
   */
  public static CachingCollector create(boolean cacheScores, double maxRAMMB) {
    Collector other = new SimpleCollector() {

      @Override
      public void collect(int doc) {}

      @Override
      public boolean needsScores() {
        return true;
      }

    };
    return create(other, cacheScores, maxRAMMB);
  }

  /**
   * Create a new {@link CachingCollector} that wraps the given collector and
   * caches documents and scores up to the specified RAM threshold.
   *
   * @param other
   *          the Collector to wrap and delegate calls to.
   * @param cacheScores
   *          whether to cache scores in addition to document IDs. Note that
   *          this increases the RAM consumed per doc
   * @param maxRAMMB
   *          the maximum RAM in MB to consume for caching the documents and
   *          scores. If the collector exceeds the threshold, no documents and
   *          scores are cached.
   */
  public static CachingCollector create(Collector other, boolean cacheScores, double maxRAMMB) {
    int bytesPerDoc = RamUsageEstimator.NUM_BYTES_INT;
    if (cacheScores) {
      bytesPerDoc += RamUsageEstimator.NUM_BYTES_FLOAT;
    }
    final int maxDocsToCache = (int) ((maxRAMMB * 1024 * 1024) / bytesPerDoc);
    return create(other, cacheScores, maxDocsToCache);
  }

  /**
   * Create a new {@link CachingCollector} that wraps the given collector and
   * caches documents and scores up to the specified max docs threshold.
   *
   * @param other
   *          the Collector to wrap and delegate calls to.
   * @param cacheScores
   *          whether to cache scores in addition to document IDs. Note that
   *          this increases the RAM consumed per doc
   * @param maxDocsToCache
   *          the maximum number of documents for caching the documents and
   *          possible the scores. If the collector exceeds the threshold,
   *          no documents and scores are cached.
   */
  public static CachingCollector create(Collector other, boolean cacheScores, int maxDocsToCache) {
    return cacheScores ? new ScoreCachingCollector(other, maxDocsToCache) : new NoScoreCachingCollector(other, maxDocsToCache);
  }

  private boolean cached;

  private CachingCollector(Collector in) {
    super(in);
    cached = true;
  }

  /**
   * Return true is this collector is able to replay collection.
   */
  public final boolean isCached() {
    return cached;
  }

  /**
   * Replays the cached doc IDs (and scores) to the given Collector. If this
   * instance does not cache scores, then Scorer is not set on
   * {@code other.setScorer} as well as scores are not replayed.
   *
   * @throws IllegalStateException
   *           if this collector is not cached (i.e., if the RAM limits were too
   *           low for the number of documents + scores to cache).
   * @throws IllegalArgumentException
   *           if the given Collect's does not support out-of-order collection,
   *           while the collector passed to the ctor does.
   */
  public abstract void replay(Collector other) throws IOException;

}
