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

import org.apache.lucene5_shaded.util.Accountable;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.RamUsageEstimator;

/**
 * Abstract decorator class for a DocIdSet implementation
 * that provides on-demand filtering/validation
 * mechanism on a given DocIdSet.
 *
 * <p>
 * Technically, this same functionality could be achieved
 * with ChainedFilter (under queries/), however the
 * benefit of this class is it never materializes the full
 * bitset for the filter.  Instead, the {@link #match}
 * method is invoked on-demand, per docID visited during
 * searching.  If you know few docIDs will be visited, and
 * the logic behind {@link #match} is relatively costly,
 * this may be a better way to filter than ChainedFilter.
 *
 * @see DocIdSet
 * @deprecated This class is not useful internally anymore and will be removed
 * in 6.0
 */
@Deprecated
public abstract class FilteredDocIdSet extends DocIdSet {
  private final DocIdSet _innerSet;
  
  /**
   * Constructor.
   * @param innerSet Underlying DocIdSet
   */
  public FilteredDocIdSet(DocIdSet innerSet) {
    _innerSet = innerSet;
  }

  /** Return the wrapped {@link DocIdSet}. */
  public DocIdSet getDelegate() {
    return _innerSet;
  }

  /** This DocIdSet implementation is cacheable if the inner set is cacheable. */
  @Override
  public boolean isCacheable() {
    return _innerSet.isCacheable();
  }

  @Override
  public long ramBytesUsed() {
    return RamUsageEstimator.NUM_BYTES_OBJECT_REF + _innerSet.ramBytesUsed();
  }
  
  @Override
  public Collection<Accountable> getChildResources() {
    return _innerSet.getChildResources();
  }

  @Override
  public Bits bits() throws IOException {
    final Bits bits = _innerSet.bits();
    return (bits == null) ? null : new Bits() {
      @Override
      public boolean get(int docid) {
        return bits.get(docid) && FilteredDocIdSet.this.match(docid);
      }

      @Override
      public int length() {
        return bits.length();
      }
    };
  }

  /**
   * Validation method to determine whether a docid should be in the result set.
   * @param docid docid to be tested
   * @return true if input docid should be in the result set, false otherwise.
   */
  protected abstract boolean match(int docid);

  /**
   * Implementation of the contract to build a DocIdSetIterator.
   * @see DocIdSetIterator
   * @see FilteredDocIdSetIterator
   */
  @Override
  public DocIdSetIterator iterator() throws IOException {
    final DocIdSetIterator iterator = _innerSet.iterator();
    if (iterator == null) {
      return null;
    }
    return new FilteredDocIdSetIterator(iterator) {
      @Override
      protected boolean match(int docid) {
        return FilteredDocIdSet.this.match(docid);
      }
    };
  }
}
