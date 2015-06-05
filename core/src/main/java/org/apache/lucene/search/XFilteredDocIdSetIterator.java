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

package org.apache.lucene.search;

import java.io.IOException;

// this is just one possible solution for "early termination" !

/**
 * Abstract decorator class of a DocIdSetIterator
 * implementation that provides on-demand filter/validation
 * mechanism on an underlying DocIdSetIterator.  See {@link
 * FilteredDocIdSet}.
 */
public abstract class XFilteredDocIdSetIterator extends DocIdSetIterator {
  protected DocIdSetIterator _innerIter;
  private int doc;

  /**
   * Constructor.
   * @param innerIter Underlying DocIdSetIterator.
   */
  public XFilteredDocIdSetIterator(DocIdSetIterator innerIter) {
    if (innerIter == null) {
      throw new IllegalArgumentException("null iterator");
    }
    _innerIter = innerIter;
    doc = -1;
  }

  /** Return the wrapped {@link DocIdSetIterator}. */
  public DocIdSetIterator getDelegate() {
    return _innerIter;
  }

  /**
   * Validation method to determine whether a docid should be in the result set.
   * @param doc docid to be tested
   * @return true if input docid should be in the result set, false otherwise.
   * @see #FilteredDocIdSetIterator(DocIdSetIterator)
   * @throws CollectionTerminatedException if the underlying iterator is exhausted.
   */
  protected abstract boolean match(int doc);

  @Override
  public int docID() {
    return doc;
  }
  
  @Override
  public int nextDoc() throws IOException {
    try {
      while ((doc = _innerIter.nextDoc()) != NO_MORE_DOCS) {
        if (match(doc)) {
          return doc;
        }
      }
    } catch (CollectionTerminatedException e) {
      return doc = NO_MORE_DOCS;
    }
    return doc;
  }
  
  @Override
  public int advance(int target) throws IOException {
    doc = _innerIter.advance(target);
    try {
      if (doc != NO_MORE_DOCS) {
        if (match(doc)) {
          return doc;
        } else {
          while ((doc = _innerIter.nextDoc()) != NO_MORE_DOCS) {
            if (match(doc)) {
              return doc;
            }
          }
          return doc;
        }
      }
    } catch (CollectionTerminatedException e) {
      return doc = NO_MORE_DOCS;
    }
    return doc;
  }

  @Override
  public long cost() {
    return _innerIter.cost();
  }
}

