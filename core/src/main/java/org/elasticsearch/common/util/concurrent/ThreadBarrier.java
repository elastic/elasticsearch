/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util.concurrent;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * A synchronization aid that allows a set of threads to all wait for each other
 * to reach a common barrier point. Barriers are useful in programs involving a
 * fixed sized party of threads that must occasionally wait for each other.
 * <code>ThreadBarrier</code> adds a <i>cause</i> to
 * {@link BrokenBarrierException} thrown by a {@link #reset()} operation defined
 * by {@link CyclicBarrier}.
 * <p/>
 * <p/>
 * <b>Sample usage:</b> <br>
 * <li>Barrier as a synchronization and Exception handling aid </li>
 * <li>Barrier as a trigger for elapsed notification events </li>
 * <p/>
 * <pre>
 *    class MyTestClass implements RemoteEventListener
 *    {
 *      final ThreadBarrier barrier;
 *
 *      class Worker implements Runnable
 *        {
 *          public void run()
 *            {
 *              barrier.await();    //wait for all threads to reach run
 *              try
 *                {
 *                  prepare();
 *                  barrier.await();    //wait for all threads to prepare
 *                  process();
 *                  barrier.await();    //wait for all threads to process
 *                }
 *              catch(Throwable t){
 *                  log(&quot;Worker thread caught exception&quot;, t);
 *                  barrier.reset(t);
 *                }
 *            }
 *        }
 *
 *      public void testThreads() {
 *          barrier = new ThreadBarrier(N_THREADS + 1);
 *          for (int i = 0; i &lt; N; ++i)
 *           new Thread(new Worker()).start();
 *
 *          try{
 *              barrier.await();    //wait for all threads to reach run
 *              barrier.await();    //wait for all threads to prepare
 *              barrier.await();    //wait for all threads to process
 *            }
 *          catch(BrokenBarrierException bbe) {
 *              Assert.fail(bbe);
 *            }
 *       }
 *
 *      int actualNotificationCount = 0;
 *      public synchronized void notify (RemoteEvent event) {
 *          try{
 *              actualNotificationCount++;
 *              if (actualNotificationCount == EXPECTED_COUNT)
 *                  barrier.await();    //signal when all notifications arrive
 *
 *               // too many notifications?
 *               Assert.assertFalse(&quot;Exceeded notification count&quot;,
 *                                          actualNotificationCount > EXPECTED_COUNT);
 *            }
 *          catch(Throwable t) {
 *              log(&quot;Worker thread caught exception&quot;, t);
 *              barrier.reset(t);
 *            }
 *        }
 *
 *      public void testNotify() {
 *          barrier = new ThreadBarrier(N_LISTENERS + 1);
 *          registerNotification();
 *          triggerNotifications();
 *
 *          //wait until either all notifications arrive, or
 *          //until a MAX_TIMEOUT is reached.
 *          barrier.await(MAX_TIMEOUT);
 *
 *          //check if all notifications were accounted for or timed-out
 *          Assert.assertEquals(&quot;Notification count&quot;,
 *                                      EXPECTED_COUNT, actualNotificationCount);
 *
 *          //inspect that the barrier isn't broken
 *          barrier.inspect(); //throws BrokenBarrierException if broken
 *        }
 *    }
 * </pre>
 *
 *
 */
public class ThreadBarrier extends CyclicBarrier {
    /**
     * The cause of a {@link BrokenBarrierException} and {@link TimeoutException}
     * thrown from an await() when {@link #reset(Throwable)} was invoked.
     */
    private Throwable cause;

    public ThreadBarrier(int parties) {
        super(parties);
    }

    public ThreadBarrier(int parties, Runnable barrierAction) {
        super(parties, barrierAction);
    }

    @Override
    public int await() throws InterruptedException, BrokenBarrierException {
        try {
            breakIfBroken();
            return super.await();
        } catch (BrokenBarrierException bbe) {
            initCause(bbe);
            throw bbe;
        }
    }

    @Override
    public int await(long timeout, TimeUnit unit) throws InterruptedException, BrokenBarrierException, TimeoutException {
        try {
            breakIfBroken();
            return super.await(timeout, unit);
        } catch (BrokenBarrierException bbe) {
            initCause(bbe);
            throw bbe;
        } catch (TimeoutException te) {
            initCause(te);
            throw te;
        }
    }

    /**
     * Resets the barrier to its initial state.  If any parties are
     * currently waiting at the barrier, they will return with a
     * {@link BrokenBarrierException}. Note that resets <em>after</em>
     * a breakage has occurred for other reasons can be complicated to
     * carry out; threads need to re-synchronize in some other way,
     * and choose one to perform the reset.  It may be preferable to
     * instead create a new barrier for subsequent use.
     *
     * @param cause The cause of the BrokenBarrierException
     */
    public synchronized void reset(Throwable cause) {
        if (!isBroken()) {
            super.reset();
        }

        if (this.cause == null) {
            this.cause = cause;
        }
    }

    /**
     * Queries if this barrier is in a broken state. Note that if
     * {@link #reset(Throwable)} is invoked the barrier will remain broken, while
     * {@link #reset()} will reset the barrier to its initial state and
     * {@link #isBroken()} will return false.
     *
     * @return {@code true} if one or more parties broke out of this barrier due
     *         to interruption or timeout since construction or the last reset,
     *         or a barrier action failed due to an exception; {@code false}
     *         otherwise.
     * @see #inspect()
     */
    @Override
    public synchronized boolean isBroken() {
        return this.cause != null || super.isBroken();
    }

    /**
     * Inspects if the barrier is broken. If for any reason, the barrier
     * was broken, a {@link BrokenBarrierException} will be thrown. Otherwise,
     * would return gracefully.
     *
     * @throws BrokenBarrierException With a nested broken cause.
     */
    public synchronized void inspect() throws BrokenBarrierException {
        try {
            breakIfBroken();
        } catch (BrokenBarrierException bbe) {
            initCause(bbe);
            throw bbe;
        }
    }

    /**
     * breaks this barrier if it has been reset or broken for any other reason.
     * <p/>
     * Note: This call is not atomic in respect to await/reset calls. A
     * breakIfBroken() may be context switched to invoke a reset() prior to
     * await(). This resets the barrier to its initial state - parties not
     * currently waiting at the barrier will not be accounted for! An await that
     * wasn't time limited, will block indefinitely.
     *
     * @throws BrokenBarrierException an empty BrokenBarrierException.
     */
    private synchronized void breakIfBroken()
            throws BrokenBarrierException {
        if (isBroken()) {
            throw new BrokenBarrierException();
        }
    }

    /**
     * Initializes the cause of this throwable to the specified value. The cause
     * is the throwable that was initialized by {@link #reset(Throwable)}.
     *
     * @param t throwable.
     */
    private synchronized void initCause(Throwable t) {
        t.initCause(this.cause);
    }

    /**
     * A Barrier action to be used in conjunction with {@link ThreadBarrier} to
     * measure performance between barrier awaits. This runnable will execute
     * when the barrier is tripped. Make sure to reset() the timer before next
     * Measurement.
     *
     * @see ThreadBarrier#ThreadBarrier(int, Runnable)
     *      <p/>
     *      <B>Usage example:</B><br>
     *      <pre><code>
     *                                                                                             BarrierTimer timer = new BarrierTimer();
     *                                                                                             ThreadBarrier barrier = new ThreadBarrier( nTHREADS + 1, timer );
     *                                                                                             ..
     *                                                                                             barrier.await(); // starts timer when all threads trip on await
     *                                                                                             barrier.await(); // stops  timer when all threads trip on await
     *                                                                                             ..
     *                                                                                             long time = timer.getTimeInNanos();
     *                                                                                             long tpi = time / ((long)nREPEATS * nTHREADS); //throughput per thread iteration
     *                                                                                             long secs = timer.getTimeInSeconds();    //total runtime in seconds
     *                                                                                             ..
     *                                                                                             timer.reset();  // reuse timer
     *                                                                                           </code></pre>
     */
    public static class BarrierTimer implements Runnable {
        volatile boolean started;
        volatile long startTime;
        volatile long endTime;

        @Override
        public void run() {
            long t = System.nanoTime();
            if (!started) {
                started = true;
                startTime = t;
            } else
                endTime = t;
        }

        /**
         * resets (clears) this timer before next execution.
         */
        public void reset() {
            started = false;
        }

        /**
         * Returns the elapsed time between two successive barrier executions.
         *
         * @return elapsed time in nanoseconds.
         */
        public long getTimeInNanos() {
            return endTime - startTime;
        }

        /**
         * Returns the elapsed time between two successive barrier executions.
         *
         * @return elapsed time in seconds.
         */
        public double getTimeInSeconds() {
            long time = endTime - startTime;
            return (time) / 1000000000.0;
        }
    }
}
