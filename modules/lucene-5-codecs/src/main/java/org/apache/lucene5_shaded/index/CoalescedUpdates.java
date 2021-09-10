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


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene5_shaded.index.BufferedUpdatesStream.QueryAndLimit;
import org.apache.lucene5_shaded.index.DocValuesUpdate.BinaryDocValuesUpdate;
import org.apache.lucene5_shaded.index.DocValuesUpdate.NumericDocValuesUpdate;
import org.apache.lucene5_shaded.search.Query;
import org.apache.lucene5_shaded.util.BytesRef;

class CoalescedUpdates {
  final Map<Query,Integer> queries = new HashMap<>();
  final List<PrefixCodedTerms> terms = new ArrayList<>();
  final List<List<DocValuesUpdate>> numericDVUpdates = new ArrayList<>();
  final List<List<DocValuesUpdate>> binaryDVUpdates = new ArrayList<>();
  long totalTermCount;
  
  @Override
  public String toString() {
    // note: we could add/collect more debugging information
    return "CoalescedUpdates(termSets=" + terms.size()
      + ",totalTermCount=" + totalTermCount
      + ",queries=" + queries.size() + ",numericDVUpdates=" + numericDVUpdates.size()
      + ",binaryDVUpdates=" + binaryDVUpdates.size() + ")";
  }

  void update(FrozenBufferedUpdates in) {
    totalTermCount += in.terms.size();
    terms.add(in.terms);

    for (int queryIdx = 0; queryIdx < in.queries.length; queryIdx++) {
      final Query query = in.queries[queryIdx];
      queries.put(query, BufferedUpdates.MAX_INT);
    }

    List<DocValuesUpdate> numericPacket = new ArrayList<>();
    numericDVUpdates.add(numericPacket);
    for (NumericDocValuesUpdate nu : in.numericDVUpdates) {
      NumericDocValuesUpdate clone = new NumericDocValuesUpdate(nu.term, nu.field, (Long) nu.value);
      clone.docIDUpto = Integer.MAX_VALUE;
      numericPacket.add(clone);
    }
    
    List<DocValuesUpdate> binaryPacket = new ArrayList<>();
    binaryDVUpdates.add(binaryPacket);
    for (BinaryDocValuesUpdate bu : in.binaryDVUpdates) {
      BinaryDocValuesUpdate clone = new BinaryDocValuesUpdate(bu.term, bu.field, (BytesRef) bu.value);
      clone.docIDUpto = Integer.MAX_VALUE;
      binaryPacket.add(clone);
    }
  }

  public FieldTermIterator termIterator() {
    if (terms.size() == 1) {
      return terms.get(0).iterator();
    } else {
      return new MergedPrefixCodedTermsIterator(terms);
    }
  }

  public Iterable<QueryAndLimit> queriesIterable() {
    return new Iterable<QueryAndLimit>() {
      
      @Override
      public Iterator<QueryAndLimit> iterator() {
        return new Iterator<QueryAndLimit>() {
          private final Iterator<Map.Entry<Query,Integer>> iter = queries.entrySet().iterator();

          @Override
          public boolean hasNext() {
            return iter.hasNext();
          }

          @Override
          public QueryAndLimit next() {
            final Map.Entry<Query,Integer> ent = iter.next();
            return new QueryAndLimit(ent.getKey(), ent.getValue());
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }
}
