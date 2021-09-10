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

/**
 * An approximation of a {@link DocIdSetIterator}. When the {@link #approximation()}'s
 * {@link DocIdSetIterator#nextDoc()} or {@link DocIdSetIterator#advance(int)}
 * return, {@link #matches()} needs to be checked in order to know whether the
 * returned doc ID actually matches.
 * @lucene.experimental
 */
public abstract class TwoPhaseDocIdSetIterator {

  /** Return a {@link DocIdSetIterator} view of the provided
   *  {@link TwoPhaseDocIdSetIterator}. */
  public static DocIdSetIterator asDocIdSetIterator(final TwoPhaseDocIdSetIterator twoPhaseIterator) {
    final DocIdSetIterator approximation = twoPhaseIterator.approximation();
    return new DocIdSetIterator() {

      @Override
      public int docID() {
        return approximation.docID();
      }

      @Override
      public int nextDoc() throws IOException {
        return doNext(approximation.nextDoc());
      }

      @Override
      public int advance(int target) throws IOException {
        return doNext(approximation.advance(target));
      }

      private int doNext(int doc) throws IOException {
        for (;; doc = approximation.nextDoc()) {
          if (doc == NO_MORE_DOCS) {
            return NO_MORE_DOCS;
          } else if (twoPhaseIterator.matches()) {
            return doc;
          }
        }
      }

      @Override
      public long cost() {
        return approximation.cost();
      }

    };
  }

  /** Return an approximation. The returned {@link DocIdSetIterator} is a
   *  superset of the matching documents, and each match needs to be confirmed
   *  with {@link #matches()} in order to know whether it matches or not. */
  public abstract DocIdSetIterator approximation();

  /** Return whether the current doc ID that the iterator is on matches. This
   *  method should only be called when the iterator is positionned -- ie. not
   *  when {@link DocIdSetIterator#docID()} is {@code -1} or
   *  {@link DocIdSetIterator#NO_MORE_DOCS} -- and at most once. */
  public abstract boolean matches() throws IOException;

}
