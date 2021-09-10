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


import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.lucene5_shaded.index.DirectoryReader;
import org.apache.lucene5_shaded.store.AlreadyClosedException;
import org.apache.lucene5_shaded.util.IOUtils;

/**
 * Keeps track of current plus old IndexSearchers, closing
 * the old ones once they have timed out.
 *
 * Use it like this:
 *
 * <pre class="prettyprint">
 *   SearcherLifetimeManager mgr = new SearcherLifetimeManager();
 * </pre>
 *
 * Per search-request, if it's a "new" search request, then
 * obtain the latest searcher you have (for example, by
 * using {@link SearcherManager}), and then record this
 * searcher:
 *
 * <pre class="prettyprint">
 *   // Record the current searcher, and save the returend
 *   // token into user's search results (eg as a  hidden
 *   // HTML form field):
 *   long token = mgr.record(searcher);
 * </pre>
 *
 * When a follow-up search arrives, for example the user
 * clicks next page, drills down/up, etc., take the token
 * that you saved from the previous search and:
 *
 * <pre class="prettyprint">
 *   // If possible, obtain the same searcher as the last
 *   // search:
 *   IndexSearcher searcher = mgr.acquire(token);
 *   if (searcher != null) {
 *     // Searcher is still here
 *     try {
 *       // do searching...
 *     } finally {
 *       mgr.release(searcher);
 *       // Do not use searcher after this!
 *       searcher = null;
 *     }
 *   } else {
 *     // Searcher was pruned -- notify user session timed
 *     // out, or, pull fresh searcher again
 *   }
 * </pre>
 *
 * Finally, in a separate thread, ideally the same thread
 * that's periodically reopening your searchers, you should
 * periodically prune old searchers:
 *
 * <pre class="prettyprint">
 *   mgr.prune(new PruneByAge(600.0));
 * </pre>
 *
 * <p><b>NOTE</b>: keeping many searchers around means
 * you'll use more resources (open files, RAM) than a single
 * searcher.  However, as long as you are using {@link
 * DirectoryReader#openIfChanged(DirectoryReader)}, the searchers
 * will usually share almost all segments and the added resource usage
 * is contained.  When a large merge has completed, and
 * you reopen, because that is a large change, the new
 * searcher will use higher additional RAM than other
 * searchers; but large merges don't complete very often and
 * it's unlikely you'll hit two of them in your expiration
 * window.  Still you should budget plenty of heap in the
 * JVM to have a good safety margin.
 * 
 * @lucene.experimental
 */

public class SearcherLifetimeManager implements Closeable {

  static final double NANOS_PER_SEC = 1000000000.0;

  private static class SearcherTracker implements Comparable<SearcherTracker>, Closeable {
    public final IndexSearcher searcher;
    public final double recordTimeSec;
    public final long version;

    public SearcherTracker(IndexSearcher searcher) {
      this.searcher = searcher;
      version = ((DirectoryReader) searcher.getIndexReader()).getVersion();
      searcher.getIndexReader().incRef();
      // Use nanoTime not currentTimeMillis since it [in
      // theory] reduces risk from clock shift
      recordTimeSec = System.nanoTime() / NANOS_PER_SEC;
    }

    // Newer searchers are sort before older ones:
    @Override
    public int compareTo(SearcherTracker other) {
      return Double.compare(other.recordTimeSec, recordTimeSec);
    }

    @Override
    public synchronized void close() throws IOException {
      searcher.getIndexReader().decRef();
    }
  }

  private volatile boolean closed;

  // TODO: we could get by w/ just a "set"; need to have
  // Tracker hash by its version and have compareTo(Long)
  // compare to its version
  private final ConcurrentHashMap<Long,SearcherTracker> searchers = new ConcurrentHashMap<>();

  private void ensureOpen() {
    if (closed) {
      throw new AlreadyClosedException("this SearcherLifetimeManager instance is closed");
    }
  }

  /** Records that you are now using this IndexSearcher.
   *  Always call this when you've obtained a possibly new
   *  {@link IndexSearcher}, for example from {@link
   *  SearcherManager}.  It's fine if you already passed the
   *  same searcher to this method before.
   *
   *  <p>This returns the long token that you can later pass
   *  to {@link #acquire} to retrieve the same IndexSearcher.
   *  You should record this long token in the search results
   *  sent to your user, such that if the user performs a
   *  follow-on action (clicks next page, drills down, etc.)
   *  the token is returned. */
  public long record(IndexSearcher searcher) throws IOException {
    ensureOpen();
    // TODO: we don't have to use IR.getVersion to track;
    // could be risky (if it's buggy); we could get better
    // bug isolation if we assign our own private ID:
    final long version = ((DirectoryReader) searcher.getIndexReader()).getVersion();
    SearcherTracker tracker = searchers.get(version);
    if (tracker == null) {
      //System.out.println("RECORD version=" + version + " ms=" + System.currentTimeMillis());
      tracker = new SearcherTracker(searcher);
      if (searchers.putIfAbsent(version, tracker) != null) {
        // Another thread beat us -- must decRef to undo
        // incRef done by SearcherTracker ctor:
        tracker.close();
      }
    } else if (tracker.searcher != searcher) {
      throw new IllegalArgumentException("the provided searcher has the same underlying reader version yet the searcher instance differs from before (new=" + searcher + " vs old=" + tracker.searcher);
    }

    return version;
  }

  /** Retrieve a previously recorded {@link IndexSearcher}, if it
   *  has not yet been closed
   *
   *  <p><b>NOTE</b>: this may return null when the
   *  requested searcher has already timed out.  When this
   *  happens you should notify your user that their session
   *  timed out and that they'll have to restart their
   *  search.
   *
   *  <p>If this returns a non-null result, you must match
   *  later call {@link #release} on this searcher, best
   *  from a finally clause. */
  public IndexSearcher acquire(long version) {
    ensureOpen();
    final SearcherTracker tracker = searchers.get(version);
    if (tracker != null &&
        tracker.searcher.getIndexReader().tryIncRef()) {
      return tracker.searcher;
    }

    return null;
  }

  /** Release a searcher previously obtained from {@link
   *  #acquire}.
   * 
   * <p><b>NOTE</b>: it's fine to call this after close. */
  public void release(IndexSearcher s) throws IOException {
    s.getIndexReader().decRef();
  }

  /** See {@link #prune}. */
  public interface Pruner {
    /** Return true if this searcher should be removed. 
     *  @param ageSec how much time has passed since this
     *         searcher was the current (live) searcher
     *  @param searcher Searcher
     **/
    public boolean doPrune(double ageSec, IndexSearcher searcher);
  }

  /** Simple pruner that drops any searcher older by
   *  more than the specified seconds, than the newest
   *  searcher. */
  public final static class PruneByAge implements Pruner {
    private final double maxAgeSec;

    public PruneByAge(double maxAgeSec) {
      if (maxAgeSec < 0) {
        throw new IllegalArgumentException("maxAgeSec must be > 0 (got " + maxAgeSec + ")");
      }
      this.maxAgeSec = maxAgeSec;
    }

    @Override
    public boolean doPrune(double ageSec, IndexSearcher searcher) {
      return ageSec > maxAgeSec;
    }
  }

  /** Calls provided {@link Pruner} to prune entries.  The
   *  entries are passed to the Pruner in sorted (newest to
   *  oldest IndexSearcher) order.
   * 
   *  <p><b>NOTE</b>: you must peridiocally call this, ideally
   *  from the same background thread that opens new
   *  searchers. */
  public synchronized void prune(Pruner pruner) throws IOException {
    // Cannot just pass searchers.values() to ArrayList ctor
    // (not thread-safe since the values can change while
    // ArrayList is init'ing itself); must instead iterate
    // ourselves:
    final List<SearcherTracker> trackers = new ArrayList<>();
    for(SearcherTracker tracker : searchers.values()) {
      trackers.add(tracker);
    }
    Collections.sort(trackers);
    double lastRecordTimeSec = 0.0;
    final double now = System.nanoTime()/NANOS_PER_SEC;
    for (SearcherTracker tracker: trackers) {
      final double ageSec;
      if (lastRecordTimeSec == 0.0) {
        ageSec = 0.0;
      } else {
        ageSec = now - lastRecordTimeSec;
      }
      // First tracker is always age 0.0 sec, since it's
      // still "live"; second tracker's age (= seconds since
      // it was "live") is now minus first tracker's
      // recordTime, etc:
      if (pruner.doPrune(ageSec, tracker.searcher)) {
        //System.out.println("PRUNE version=" + tracker.version + " age=" + ageSec + " ms=" + System.currentTimeMillis());
        searchers.remove(tracker.version);
        tracker.close();
      }
      lastRecordTimeSec = tracker.recordTimeSec;
    }
  }

  /** Close this to future searching; any searches still in
   *  process in other threads won't be affected, and they
   *  should still call {@link #release} after they are
   *  done.
   *
   *  <p><b>NOTE</b>: you must ensure no other threads are
   *  calling {@link #record} while you call close();
   *  otherwise it's possible not all searcher references
   *  will be freed. */
  @Override
  public synchronized void close() throws IOException {
    closed = true;
    final List<SearcherTracker> toClose = new ArrayList<>(searchers.values());

    // Remove up front in case exc below, so we don't
    // over-decRef on double-close:
    for(SearcherTracker tracker : toClose) {
      searchers.remove(tracker.version);
    }

    IOUtils.close(toClose);

    // Make some effort to catch mis-use:
    if (searchers.size() != 0) {
      throw new IllegalStateException("another thread called record while this SearcherLifetimeManager instance was being closed; not all searchers were closed");
    }
  }
}
