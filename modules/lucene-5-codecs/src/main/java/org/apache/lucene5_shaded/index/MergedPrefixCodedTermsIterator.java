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


import java.util.List;

import org.apache.lucene5_shaded.index.PrefixCodedTerms.TermIterator;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.PriorityQueue;

/** Merges multiple {@link FieldTermIterator}s */
class MergedPrefixCodedTermsIterator extends FieldTermIterator {

  private static class TermMergeQueue extends PriorityQueue<TermIterator> {
    TermMergeQueue(int size) {
      super(size);
    }
    
    @Override
    protected boolean lessThan(TermIterator a, TermIterator b) {
      int cmp = a.bytes.compareTo(b.bytes);
      if (cmp < 0) {
        return true;
      } else if (cmp > 0) {
        return false;
      } else {
        return a.delGen() > b.delGen();
      }
    }
  }

  private static class FieldMergeQueue extends PriorityQueue<TermIterator> {
    FieldMergeQueue(int size) {
      super(size);
    }
    
    @Override
    protected boolean lessThan(TermIterator a, TermIterator b) {
      return a.field.compareTo(b.field) < 0;
    }
  }

  final TermMergeQueue termQueue;
  final FieldMergeQueue fieldQueue;

  public MergedPrefixCodedTermsIterator(List<PrefixCodedTerms> termsList) {
    fieldQueue = new FieldMergeQueue(termsList.size());
    for (PrefixCodedTerms terms : termsList) {
      TermIterator iter = terms.iterator();
      iter.next();
      if (iter.field != null) {
        fieldQueue.add(iter);
      }
    }

    termQueue = new TermMergeQueue(termsList.size());
  }

  String field;

  @Override
  public BytesRef next() {
    if (termQueue.size() == 0) {
      // No more terms in current field:
      if (fieldQueue.size() == 0) {
        // No more fields:
        field = null;
        return null;
      }

      // Transfer all iterators on the next field into the term queue:
      TermIterator top = fieldQueue.pop();
      termQueue.add(top);
      field = top.field;
      assert field != null;

      while (fieldQueue.size() != 0 && fieldQueue.top().field.equals(top.field)) {
        TermIterator iter = fieldQueue.pop();
        assert iter.field.equals(field);
        // TODO: a little bit evil; we do this so we can == on field down below:
        iter.field = field;
        termQueue.add(iter);
      }

      return termQueue.top().bytes;
    } else {
      TermIterator top = termQueue.top();
      if (top.next() == null) {
        termQueue.pop();
      } else if (top.field() != field) {
        // Field changed
        termQueue.pop();
        fieldQueue.add(top);
      } else {
        termQueue.updateTop();
      }
      if (termQueue.size() == 0) {
        // Recurse (just once) to go to next field:                                                                                                                                        
        return next();
      } else {
        // Still terms left in this field
        return termQueue.top().bytes;
      }
    }
  }

  @Override
  public String field() {
    return field;
  }

  @Override
  public long delGen() {
    return termQueue.top().delGen();
  }
}

