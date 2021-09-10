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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.lucene5_shaded.index.IndexWriter;
import org.apache.lucene5_shaded.index.TrackingIndexWriter;
import org.apache.lucene5_shaded.util.ThreadInterruptedException;

/** Utility class that runs a thread to manage periodicc
 *  reopens of a {@link ReferenceManager}, with methods to wait for a specific
 *  index changes to become visible.  To use this class you
 *  must first wrap your {@link IndexWriter} with a {@link
 *  TrackingIndexWriter} and always use it to make changes
 *  to the index, saving the returned generation.  Then,
 *  when a given search request needs to see a specific
 *  index change, call the {#waitForGeneration} to wait for
 *  that change to be visible.  Note that this will only
 *  scale well if most searches do not need to wait for a
 *  specific index generation.
 *
 * @lucene.experimental */

public class ControlledRealTimeReopenThread<T> extends Thread implements Closeable {
  private final ReferenceManager<T> manager;
  private final long targetMaxStaleNS;
  private final long targetMinStaleNS;
  private final TrackingIndexWriter writer;
  private volatile boolean finish;
  private volatile long waitingGen;
  private volatile long searchingGen;
  private long refreshStartGen;

  private final ReentrantLock reopenLock = new ReentrantLock();
  private final Condition reopenCond = reopenLock.newCondition();
  
  /**
   * Create ControlledRealTimeReopenThread, to periodically
   * reopen the a {@link ReferenceManager}.
   *
   * @param targetMaxStaleSec Maximum time until a new
   *        reader must be opened; this sets the upper bound
   *        on how slowly reopens may occur, when no
   *        caller is waiting for a specific generation to
   *        become visible.
   *
   * @param targetMinStaleSec Mininum time until a new
   *        reader can be opened; this sets the lower bound
   *        on how quickly reopens may occur, when a caller
   *        is waiting for a specific generation to
   *        become visible.
   */
  public ControlledRealTimeReopenThread(TrackingIndexWriter writer, ReferenceManager<T> manager, double targetMaxStaleSec, double targetMinStaleSec) {
    if (targetMaxStaleSec < targetMinStaleSec) {
      throw new IllegalArgumentException("targetMaxScaleSec (= " + targetMaxStaleSec + ") < targetMinStaleSec (=" + targetMinStaleSec + ")");
    }
    this.writer = writer;
    this.manager = manager;
    this.targetMaxStaleNS = (long) (1000000000*targetMaxStaleSec);
    this.targetMinStaleNS = (long) (1000000000*targetMinStaleSec);
    manager.addListener(new HandleRefresh());
  }

  private class HandleRefresh implements ReferenceManager.RefreshListener {
    @Override
    public void beforeRefresh() {
    }

    @Override
    public void afterRefresh(boolean didRefresh) {
      refreshDone();
    }
  }

  private synchronized void refreshDone() {
    searchingGen = refreshStartGen;
    notifyAll();
  }

  @Override
  public synchronized void close() {
    //System.out.println("NRT: set finish");

    finish = true;

    // So thread wakes up and notices it should finish:
    reopenLock.lock();
    try {
      reopenCond.signal();
    } finally {
      reopenLock.unlock();
    }

    try {
      join();
    } catch (InterruptedException ie) {
      throw new ThreadInterruptedException(ie);
    }

    // Max it out so any waiting search threads will return:
    searchingGen = Long.MAX_VALUE;
    notifyAll();
  }

  /**
   * Waits for the target generation to become visible in
   * the searcher.
   * If the current searcher is older than the
   * target generation, this method will block
   * until the searcher is reopened, by another via
   * {@link ReferenceManager#maybeRefresh} or until the {@link ReferenceManager} is closed.
   * 
   * @param targetGen the generation to wait for
   */
  public void waitForGeneration(long targetGen) throws InterruptedException {
    waitForGeneration(targetGen, -1);
  }

  /**
   * Waits for the target generation to become visible in
   * the searcher, up to a maximum specified milli-seconds.
   * If the current searcher is older than the target
   * generation, this method will block until the
   * searcher has been reopened by another thread via
   * {@link ReferenceManager#maybeRefresh}, the given waiting time has elapsed, or until
   * the {@link ReferenceManager} is closed.
   * <p>
   * NOTE: if the waiting time elapses before the requested target generation is
   * available the current {@link SearcherManager} is returned instead.
   * 
   * @param targetGen
   *          the generation to wait for
   * @param maxMS
   *          maximum milliseconds to wait, or -1 to wait indefinitely
   * @return true if the targetGeneration is now available,
   *         or false if maxMS wait time was exceeded
   */
  public synchronized boolean waitForGeneration(long targetGen, int maxMS) throws InterruptedException {
    final long curGen = writer.getGeneration();
    if (targetGen > curGen) {
      throw new IllegalArgumentException("targetGen=" + targetGen + " was never returned by the ReferenceManager instance (current gen=" + curGen + ")");
    }
    if (targetGen > searchingGen) {
      // Notify the reopen thread that the waitingGen has
      // changed, so it may wake up and realize it should
      // not sleep for much or any longer before reopening:
      reopenLock.lock();

      // Need to find waitingGen inside lock as it's used to determine
      // stale time
      waitingGen = Math.max(waitingGen, targetGen);

      try {
        reopenCond.signal();
      } finally {
        reopenLock.unlock();
      }

      long startMS = System.nanoTime()/1000000;

      while (targetGen > searchingGen) {
        if (maxMS < 0) {
          wait();
        } else {
          long msLeft = (startMS + maxMS) - System.nanoTime()/1000000;
          if (msLeft <= 0) {
            return false;
          } else {
            wait(msLeft);
          }
        }
      }
    }

    return true;
  }

  @Override
  public void run() {
    // TODO: maybe use private thread ticktock timer, in
    // case clock shift messes up nanoTime?
    long lastReopenStartNS = System.nanoTime();

    //System.out.println("reopen: start");
    while (!finish) {

      // TODO: try to guestimate how long reopen might
      // take based on past data?

      // Loop until we've waiting long enough before the
      // next reopen:
      while (!finish) {

        // Need lock before finding out if has waiting
        reopenLock.lock();
        try {
          // True if we have someone waiting for reopened searcher:
          boolean hasWaiting = waitingGen > searchingGen;
          final long nextReopenStartNS = lastReopenStartNS + (hasWaiting ? targetMinStaleNS : targetMaxStaleNS);

          final long sleepNS = nextReopenStartNS - System.nanoTime();

          if (sleepNS > 0) {
            reopenCond.awaitNanos(sleepNS);
          } else {
            break;
          }
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        } finally {
          reopenLock.unlock();
        }
      }

      if (finish) {
        break;
      }

      lastReopenStartNS = System.nanoTime();
      // Save the gen as of when we started the reopen; the
      // listener (HandleRefresh above) copies this to
      // searchingGen once the reopen completes:
      refreshStartGen = writer.getAndIncrementGeneration();
      try {
        manager.maybeRefreshBlocking();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  /** Returns which {@code generation} the current searcher is guaranteed to include. */
  public long getSearchingGen() {
    return searchingGen;
  }
}
