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
package org.apache.lucene5_shaded.index;


import java.util.concurrent.TimeUnit;

import static java.lang.System.nanoTime;

/**
 * An implementation of {@link QueryTimeout} that can be used by
 * the {@link ExitableDirectoryReader} class to time out and exit out
 * when a query takes a long time to rewrite.
 */
public class QueryTimeoutImpl implements QueryTimeout {

  /**
   * The local variable to store the time beyond which, the processing should exit.
   */
  private Long timeoutAt;

  /** 
   * Sets the time at which to time out by adding the given timeAllowed to the current time.
   * 
   * @param timeAllowed Number of milliseconds after which to time out. Use {@code Long.MAX_VALUE}
   *                    to effectively never time out.
   */                    
  public QueryTimeoutImpl(long timeAllowed) {
    if (timeAllowed < 0L) {
      timeAllowed = Long.MAX_VALUE;
    }
    timeoutAt = nanoTime() + TimeUnit.NANOSECONDS.convert(timeAllowed, TimeUnit.MILLISECONDS);
  }

  /**
   * Returns time at which to time out, in nanoseconds relative to the (JVM-specific)
   * epoch for {@link System#nanoTime()}, to compare with the value returned by
   * {@code nanoTime()}.
   */
  public Long getTimeoutAt() {
    return timeoutAt;
  }

  /**
   * Return true if {@link #reset()} has not been called
   * and the elapsed time has exceeded the time allowed.
   */
  @Override
  public boolean shouldExit() {
    return timeoutAt != null && nanoTime() - timeoutAt > 0;
  }

  /**
   * Reset the timeout value.
   */
  public void reset() {
    timeoutAt = null;
  }
  
  @Override
  public String toString() {
    return "timeoutAt: " + timeoutAt + " (System.nanoTime(): " + nanoTime() + ")";
  }
}


