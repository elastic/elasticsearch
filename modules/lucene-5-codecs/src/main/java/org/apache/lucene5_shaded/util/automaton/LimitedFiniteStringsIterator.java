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
package org.apache.lucene5_shaded.util.automaton;


import org.apache.lucene5_shaded.util.IntsRef;

/**
 * {@link FiniteStringsIterator} which limits the number of iterated accepted strings.
 * If more than <code>limit</code> strings are accepted,
 * the first <code>limit</code> strings found are returned.
 *
 * <p>If the {@link Automaton} has cycles then this iterator may throw an {@code
 * IllegalArgumentException}, but this is not guaranteed!
 *
 * <p>Be aware that the iteration order is implementation dependent
 * and may change across releases.
 *
 * @lucene.experimental
 */
public class LimitedFiniteStringsIterator extends FiniteStringsIterator {
  /**
   * Maximum number of finite strings to create.
   */
  private int limit = Integer.MAX_VALUE;

  /**
   * Number of generated finite strings.
   */
  private int count = 0;

  /**
   * Constructor.
   *
   * @param a Automaton to create finite string from.
   * @param limit Maximum number of finite strings to create, or -1 for infinite.
   */
  public LimitedFiniteStringsIterator(Automaton a, int limit) {
    super(a);

    if (limit != -1 && limit <= 0) {
      throw new IllegalArgumentException("limit must be -1 (which means no limit), or > 0; got: " + limit);
    }

    this.limit = limit > 0? limit : Integer.MAX_VALUE;
  }

  @Override
  public IntsRef next() {
    if (count >= limit) {
      // Abort on limit.
      return null;
    }

    IntsRef result = super.next();
    if (result != null) {
      count++;
    }

    return result;
  }

  /**
   * Number of iterated finite strings.
   */
  public int size() {
    return count;
  }
}
