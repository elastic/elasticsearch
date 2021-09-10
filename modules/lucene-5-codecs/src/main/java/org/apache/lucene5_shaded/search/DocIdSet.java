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
import java.util.Collection;
import java.util.Collections;

import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.Bits;

/**
 * A DocIdSet contains a set of doc ids. Implementing classes must
 * only implement {@link #iterator} to provide access to the set. 
 */
public abstract class DocIdSet implements Accountable {

  /** An empty {@code DocIdSet} instance */
  public static final DocIdSet EMPTY = new DocIdSet() {
    
    @Override
    public DocIdSetIterator iterator() {
      return DocIdSetIterator.empty();
    }
    
    @Override
    public boolean isCacheable() {
      return true;
    }
    
    // we explicitly provide no random access, as this filter is 100% sparse and iterator exits faster
    @Override
    public Bits bits() {
      return null;
    }

    @Override
    public long ramBytesUsed() {
      return 0L;
    }
  };

  /** Provides a {@link DocIdSetIterator} to access the set.
   * This implementation can return <code>null</code> if there
   * are no docs that match. */
  public abstract DocIdSetIterator iterator() throws IOException;

  // TODO: somehow this class should express the cost of
  // iteration vs the cost of random access Bits; for
  // expensive Filters (e.g. distance < 1 km) we should use
  // bits() after all other Query/Filters have matched, but
  // this is the opposite of what bits() is for now
  // (down-low filtering using e.g. FixedBitSet)

  /** Optionally provides a {@link Bits} interface for random access
   * to matching documents.
   * @return {@code null}, if this {@code DocIdSet} does not support random access.
   * In contrast to {@link #iterator()}, a return value of {@code null}
   * <b>does not</b> imply that no documents match the filter!
   * The default implementation does not provide random access, so you
   * only need to implement this method if your DocIdSet can
   * guarantee random access to every docid in O(1) time without
   * external disk access (as {@link Bits} interface cannot throw
   * {@link IOException}). This is generally true for bit sets
   * like {@link org.apache.lucene5_shaded.util.FixedBitSet}, which return
   * itself if they are used as {@code DocIdSet}.
   */
  public Bits bits() throws IOException {
    return null;
  }

  /**
   * This method is a hint for {@link CachingWrapperFilter}, if this <code>DocIdSet</code>
   * should be cached without copying it. The default is to return
   * <code>false</code>. If you have an own <code>DocIdSet</code> implementation
   * that does its iteration very effective and fast without doing disk I/O,
   * override this method and return <code>true</code>.
   */
  public boolean isCacheable() {
    return false;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Collections.emptyList();
  }
}
