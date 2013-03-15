/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.apache.lucene.store;

import org.apache.lucene.util.ThreadInterruptedException;

// LUCENE UPGRADE - this is a copy of a RateLimiter.SimpleRateLimiter fixing bug #2785 Lucene 4.3 should fix that
public final class XSimpleRateLimiter extends RateLimiter {
    private volatile double mbPerSec;
    private volatile double nsPerByte;
    private volatile long lastNS;

    // TODO: we could also allow eg a sub class to dynamically
    // determine the allowed rate, eg if an app wants to
    // change the allowed rate over time or something

    /** mbPerSec is the MB/sec max IO rate */
    public XSimpleRateLimiter(double mbPerSec) {
      setMbPerSec(mbPerSec);
    }

    /**
     * Sets an updated mb per second rate limit.
     */
    @Override
    public void setMbPerSec(double mbPerSec) {
      this.mbPerSec = mbPerSec;
      nsPerByte = 1000000000. / (1024*1024*mbPerSec);
      
    }

    /**
     * The current mb per second rate limit.
     */
    @Override
    public double getMbPerSec() {
      return this.mbPerSec;
    }
    
    /** Pauses, if necessary, to keep the instantaneous IO
     *  rate at or below the target. NOTE: multiple threads
     *  may safely use this, however the implementation is
     *  not perfectly thread safe but likely in practice this
     *  is harmless (just means in some rare cases the rate
     *  might exceed the target).  It's best to call this
     *  with a biggish count, not one byte at a time.
     *  @return the pause time in nano seconds 
     * */
    @Override
    public long pause(long bytes) {
      if (bytes == 1) {
        return 0;
      }

      // TODO: this is purely instantaneous rate; maybe we
      // should also offer decayed recent history one?
      final long targetNS = lastNS = lastNS + ((long) (bytes * nsPerByte));
      final long startNs;
      long curNS = startNs = System.nanoTime();
      if (lastNS < curNS) {
        lastNS = curNS;
      }

      // While loop because Thread.sleep doesn't always sleep
      // enough:
      while(true) {
        final long pauseNS = targetNS - curNS;
        if (pauseNS > 0) {
          try {
            Thread.sleep((int) (pauseNS/1000000), (int) (pauseNS % 1000000));
          } catch (InterruptedException ie) {
            throw new ThreadInterruptedException(ie);
          }
          curNS = System.nanoTime();
          continue;
        }
        break;
      }
      
      return curNS - startNs;
    }
  }