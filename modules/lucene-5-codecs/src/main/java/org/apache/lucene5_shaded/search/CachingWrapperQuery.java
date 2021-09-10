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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene5_shaded.index.IndexReader;
import org.apache.lucene5_shaded.index.LeafReader;
import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.index.Term;
import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.Accountables;
import org.apache.lucene5_shaded.util.RoaringDocIdSet;

/**
 * Wraps another {@link Query}'s result and caches it when scores are not
 * needed.  The purpose is to allow queries to simply care about matching and
 * scoring, and then wrap with this class to add caching.
 * @deprecated Use a {@link QueryCache} instead, such as {@link LRUQueryCache}.
 */
@Deprecated
public class CachingWrapperQuery extends Query implements Accountable, Cloneable {
  private Query query; // not final because of clone
  private final QueryCachingPolicy policy;
  private final Map<Object,DocIdSet> cache = Collections.synchronizedMap(new WeakHashMap<Object,DocIdSet>());

  /** Wraps another query's result and caches it according to the provided policy.
   * @param query Query to cache results of
   * @param policy policy defining which filters should be cached on which segments
   */
  public CachingWrapperQuery(Query query, QueryCachingPolicy policy) {
    this.query = Objects.requireNonNull(query, "Query must not be null");
    this.policy = Objects.requireNonNull(policy, "QueryCachingPolicy must not be null");
  }

  /** Same as {@link CachingWrapperQuery#CachingWrapperQuery(Query, QueryCachingPolicy)}
   *  but enforces the use of the
   *  {@link QueryCachingPolicy.CacheOnLargeSegments#DEFAULT} policy. */
  public CachingWrapperQuery(Query query) {
    this(query, QueryCachingPolicy.CacheOnLargeSegments.DEFAULT);
  }

  /**
   * Gets the contained query.
   * @return the contained query.
   */
  public Query getQuery() {
    return query;
  }
  
  /**
   * Default cache implementation: uses {@link RoaringDocIdSet}.
   */
  protected DocIdSet cacheImpl(DocIdSetIterator iterator, LeafReader reader) throws IOException {
    return new RoaringDocIdSet.Builder(reader.maxDoc()).add(iterator).build();
  }

  @Override
  public Query rewrite(IndexReader reader) throws IOException {
    if (getBoost() != 1f) {
      return super.rewrite(reader);
    }
    final Query rewritten = query.rewrite(reader);
    if (query == rewritten) {
      return super.rewrite(reader);
    } else {
      CachingWrapperQuery clone = (CachingWrapperQuery) clone();
      clone.query = rewritten;
      return clone;
    }
  }

  // for testing
  int hitCount, missCount;

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    final Weight weight = query.createWeight(searcher, needsScores);
    if (needsScores) {
      // our cache is not sufficient, we need scores too
      return weight;
    }

    return new ConstantScoreWeight(weight.getQuery()) {

      final AtomicBoolean used = new AtomicBoolean(false);

      @Override
      public void extractTerms(Set<Term> terms) {
        weight.extractTerms(terms);
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        if (used.compareAndSet(false, true)) {
          policy.onUse(getQuery());
        }

        final LeafReader reader = context.reader();
        final Object key = reader.getCoreCacheKey();

        DocIdSet docIdSet = cache.get(key);
        if (docIdSet != null) {
          hitCount++;
        } else if (policy.shouldCache(query, context)) {
          missCount++;
          final Scorer scorer = weight.scorer(context);
          if (scorer == null) {
            docIdSet = DocIdSet.EMPTY;
          } else {
            docIdSet = cacheImpl(scorer.iterator(), context.reader());
          }
          cache.put(key, docIdSet);
        } else {
          return weight.scorer(context);
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
    };
  }
  
  @Override
  public String toString(String field) {
    return getClass().getSimpleName() + "("+query.toString(field)+")";
  }

  @Override
  public boolean equals(Object o) {
    if (super.equals(o) == false) return false;
    final CachingWrapperQuery other = (CachingWrapperQuery) o;
    return this.query.equals(other.query);
  }

  @Override
  public int hashCode() {
    return (query.hashCode() ^ super.hashCode());
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
