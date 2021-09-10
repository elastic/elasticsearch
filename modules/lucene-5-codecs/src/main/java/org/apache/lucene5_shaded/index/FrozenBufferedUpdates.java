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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene5_shaded.index.BufferedUpdatesStream.QueryAndLimit;
import org.apache.lucene5_shaded.index.DocValuesUpdate.BinaryDocValuesUpdate;
import org.apache.lucene5_shaded.index.DocValuesUpdate.NumericDocValuesUpdate;
import org.apache.lucene5_shaded.index.PrefixCodedTerms.TermIterator;
import org.apache.lucene5_shaded.search.Query;
import org.apache.lucene5_shaded.util.ArrayUtil;
import org.apache.lucene5_shaded.util.RamUsageEstimator;

/**
 * Holds buffered deletes and updates by term or query, once pushed. Pushed
 * deletes/updates are write-once, so we shift to more memory efficient data
 * structure to hold them. We don't hold docIDs because these are applied on
 * flush.
 */
class FrozenBufferedUpdates {

  /* Query we often undercount (say 24 bytes), plus int. */
  final static int BYTES_PER_DEL_QUERY = RamUsageEstimator.NUM_BYTES_OBJECT_REF + RamUsageEstimator.NUM_BYTES_INT + 24;
  
  // Terms, in sorted order:
  final PrefixCodedTerms terms;

  // Parallel array of deleted query, and the docIDUpto for each
  final Query[] queries;
  final int[] queryLimits;
  
  // numeric DV update term and their updates
  final NumericDocValuesUpdate[] numericDVUpdates;
  
  // binary DV update term and their updates
  final BinaryDocValuesUpdate[] binaryDVUpdates;
  
  final int bytesUsed;
  final int numTermDeletes;
  private long gen = -1; // assigned by BufferedUpdatesStream once pushed
  final boolean isSegmentPrivate;  // set to true iff this frozen packet represents 
                                   // a segment private deletes. in that case is should
                                   // only have Queries 


  public FrozenBufferedUpdates(BufferedUpdates deletes, boolean isSegmentPrivate) {
    this.isSegmentPrivate = isSegmentPrivate;
    assert !isSegmentPrivate || deletes.terms.size() == 0 : "segment private package should only have del queries"; 
    Term termsArray[] = deletes.terms.keySet().toArray(new Term[deletes.terms.size()]);
    ArrayUtil.timSort(termsArray);
    PrefixCodedTerms.Builder builder = new PrefixCodedTerms.Builder();
    for (Term term : termsArray) {
      builder.add(term);
    }
    terms = builder.finish();
    
    queries = new Query[deletes.queries.size()];
    queryLimits = new int[deletes.queries.size()];
    int upto = 0;
    for(Map.Entry<Query,Integer> ent : deletes.queries.entrySet()) {
      queries[upto] = ent.getKey();
      queryLimits[upto] = ent.getValue();
      upto++;
    }

    // TODO if a Term affects multiple fields, we could keep the updates key'd by Term
    // so that it maps to all fields it affects, sorted by their docUpto, and traverse
    // that Term only once, applying the update to all fields that still need to be
    // updated. 
    List<NumericDocValuesUpdate> allNumericUpdates = new ArrayList<>();
    int numericUpdatesSize = 0;
    for (LinkedHashMap<Term,NumericDocValuesUpdate> numericUpdates : deletes.numericUpdates.values()) {
      for (NumericDocValuesUpdate update : numericUpdates.values()) {
        allNumericUpdates.add(update);
        numericUpdatesSize += update.sizeInBytes();
      }
    }
    numericDVUpdates = allNumericUpdates.toArray(new NumericDocValuesUpdate[allNumericUpdates.size()]);
    
    // TODO if a Term affects multiple fields, we could keep the updates key'd by Term
    // so that it maps to all fields it affects, sorted by their docUpto, and traverse
    // that Term only once, applying the update to all fields that still need to be
    // updated. 
    List<BinaryDocValuesUpdate> allBinaryUpdates = new ArrayList<>();
    int binaryUpdatesSize = 0;
    for (LinkedHashMap<Term,BinaryDocValuesUpdate> binaryUpdates : deletes.binaryUpdates.values()) {
      for (BinaryDocValuesUpdate update : binaryUpdates.values()) {
        allBinaryUpdates.add(update);
        binaryUpdatesSize += update.sizeInBytes();
      }
    }
    binaryDVUpdates = allBinaryUpdates.toArray(new BinaryDocValuesUpdate[allBinaryUpdates.size()]);
    
    bytesUsed = (int) (terms.ramBytesUsed() + queries.length * BYTES_PER_DEL_QUERY 
        + numericUpdatesSize + RamUsageEstimator.shallowSizeOf(numericDVUpdates)
        + binaryUpdatesSize + RamUsageEstimator.shallowSizeOf(binaryDVUpdates));
    
    numTermDeletes = deletes.numTermDeletes.get();
  }
  
  public void setDelGen(long gen) {
    assert this.gen == -1;
    this.gen = gen;
    terms.setDelGen(gen);
  }
  
  public long delGen() {
    assert gen != -1;
    return gen;
  }

  public TermIterator termIterator() {
    return terms.iterator();
  }

  public Iterable<QueryAndLimit> queriesIterable() {
    return new Iterable<QueryAndLimit>() {
      @Override
      public Iterator<QueryAndLimit> iterator() {
        return new Iterator<QueryAndLimit>() {
          private int upto;

          @Override
          public boolean hasNext() {
            return upto < queries.length;
          }

          @Override
          public QueryAndLimit next() {
            QueryAndLimit ret = new QueryAndLimit(queries[upto], queryLimits[upto]);
            upto++;
            return ret;
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  @Override
  public String toString() {
    String s = "";
    if (numTermDeletes != 0) {
      s += " " + numTermDeletes + " deleted terms (unique count=" + terms.size() + ")";
    }
    if (queries.length != 0) {
      s += " " + queries.length + " deleted queries";
    }
    if (bytesUsed != 0) {
      s += " bytesUsed=" + bytesUsed;
    }

    return s;
  }
  
  boolean any() {
    return terms.size() > 0 || queries.length > 0 || numericDVUpdates.length > 0 || binaryDVUpdates.length > 0;
  }
}
