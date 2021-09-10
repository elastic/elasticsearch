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

/**
 * A {@link Collector} which allows running a search with several
 * {@link Collector}s. It offers a static {@link #wrap} method which accepts a
 * list of collectors and wraps them with {@link MultiCollector}, while
 * filtering out the <code>null</code> null ones.
 */
public class MultiCollector implements Collector {

  /** See {@link #wrap(Iterable)}. */
  public static Collector wrap(Collector... collectors) {
    return wrap(Arrays.asList(collectors));
  }

  /**
   * Wraps a list of {@link Collector}s with a {@link MultiCollector}. This
   * method works as follows:
   * <ul>
   * <li>Filters out the <code>null</code> collectors, so they are not used
   * during search time.
   * <li>If the input contains 1 real collector (i.e. non-<code>null</code> ),
   * it is returned.
   * <li>Otherwise the method returns a {@link MultiCollector} which wraps the
   * non-<code>null</code> ones.
   * </ul>
   * 
   * @throws IllegalArgumentException
   *           if either 0 collectors were input, or all collectors are
   *           <code>null</code>.
   */
  public static Collector wrap(Iterable<? extends Collector> collectors) {
    // For the user's convenience, we allow null collectors to be passed.
    // However, to improve performance, these null collectors are found
    // and dropped from the array we save for actual collection time.
    int n = 0;
    for (Collector c : collectors) {
      if (c != null) {
        n++;
      }
    }

    if (n == 0) {
      throw new IllegalArgumentException("At least 1 collector must not be null");
    } else if (n == 1) {
      // only 1 Collector - return it.
      Collector col = null;
      for (Collector c : collectors) {
        if (c != null) {
          col = c;
          break;
        }
      }
      return col;
    } else {
      Collector[] colls = new Collector[n];
      n = 0;
      for (Collector c : collectors) {
        if (c != null) {
          colls[n++] = c;
        }
      }
      return new MultiCollector(colls);
    }
  }
  
  private final boolean cacheScores;
  private final Collector[] collectors;

  private MultiCollector(Collector... collectors) {
    this.collectors = collectors;
    int numNeedsScores = 0;
    for (Collector collector : collectors) {
      if (collector.needsScores()) {
        numNeedsScores += 1;
      }
    }
    this.cacheScores = numNeedsScores >= 2;
  }

  @Override
  public boolean needsScores() {
    for (Collector collector : collectors) {
      if (collector.needsScores()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    final List<LeafCollector> leafCollectors = new ArrayList<>();
    for (Collector collector : collectors) {
      final LeafCollector leafCollector;
      try {
        leafCollector = collector.getLeafCollector(context);
      } catch (CollectionTerminatedException e) {
        // this leaf collector does not need this segment
        continue;
      }
      leafCollectors.add(leafCollector);
    }
    switch (leafCollectors.size()) {
      case 0:
        throw new CollectionTerminatedException();
      case 1:
        return leafCollectors.get(0);
      default:
        return new MultiLeafCollector(leafCollectors, cacheScores);
    }
  }

  private static class MultiLeafCollector implements LeafCollector {

    private final boolean cacheScores;
    private final LeafCollector[] collectors;
    private int numCollectors;

    private MultiLeafCollector(List<LeafCollector> collectors, boolean cacheScores) {
      this.collectors = collectors.toArray(new LeafCollector[collectors.size()]);
      this.cacheScores = cacheScores;
      this.numCollectors = this.collectors.length;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      if (cacheScores) {
        scorer = new ScoreCachingWrappingScorer(scorer);
      }
      for (int i = 0; i < numCollectors; ++i) {
        final LeafCollector c = collectors[i];
        c.setScorer(scorer);
      }
    }

    private void removeCollector(int i) {
      System.arraycopy(collectors, i + 1, collectors, i, numCollectors - i - 1);
      --numCollectors;
      collectors[numCollectors] = null;
    }

    @Override
    public void collect(int doc) throws IOException {
      final LeafCollector[] collectors = this.collectors;
      int numCollectors = this.numCollectors;
      for (int i = 0; i < numCollectors; ) {
        final LeafCollector collector = collectors[i];
        try {
          collector.collect(doc);
          ++i;
        } catch (CollectionTerminatedException e) {
          removeCollector(i);
          numCollectors = this.numCollectors;
          if (numCollectors == 0) {
            throw new CollectionTerminatedException();
          }
        }
      }
    }

  }

}
