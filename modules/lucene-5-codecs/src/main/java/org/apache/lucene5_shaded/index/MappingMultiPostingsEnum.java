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


import java.io.IOException;

import org.apache.lucene5_shaded.index.MultiPostingsEnum.EnumWithSlice;
import org.apache.lucene5_shaded.util.BytesRef;

/**
 * Exposes flex API, merged from flex API of sub-segments,
 * remapping docIDs (this is used for segment merging).
 *
 * @lucene.experimental
 */

final class MappingMultiPostingsEnum extends PostingsEnum {
  private EnumWithSlice[] subs;
  int numSubs;
  int upto;
  MergeState.DocMap currentMap;
  PostingsEnum current;
  int currentBase;
  int doc = -1;
  private MergeState mergeState;
  MultiPostingsEnum multiDocsAndPositionsEnum;
  final String field;

  /** Sole constructor. */
  public MappingMultiPostingsEnum(String field, MergeState mergeState) {
    this.field = field;
    this.mergeState = mergeState;
  }

  MappingMultiPostingsEnum reset(MultiPostingsEnum postingsEnum) {
    this.numSubs = postingsEnum.getNumSubs();
    this.subs = postingsEnum.getSubs();
    upto = -1;
    doc = -1;
    current = null;
    this.multiDocsAndPositionsEnum = postingsEnum;
    return this;
  }

  /** How many sub-readers we are merging.
   *  @see #getSubs */
  public int getNumSubs() {
    return numSubs;
  }

  /** Returns sub-readers we are merging. */
  public EnumWithSlice[] getSubs() {
    return subs;
  }

  @Override
  public int freq() throws IOException {
    return current.freq();
  }

  @Override
  public int docID() {
    return doc;
  }

  @Override
  public int advance(int target) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int nextDoc() throws IOException {
    while(true) {
      if (current == null) {
        if (upto == numSubs-1) {
          return this.doc = NO_MORE_DOCS;
        } else {
          upto++;
          final int reader = subs[upto].slice.readerIndex;
          current = subs[upto].postingsEnum;
          currentBase = mergeState.docBase[reader];
          currentMap = mergeState.docMaps[reader];
        }
      }

      int doc = current.nextDoc();
      if (doc != NO_MORE_DOCS) {
        // compact deletions
        doc = currentMap.get(doc);
        if (doc == -1) {
          continue;
        }
        return this.doc = currentBase + doc;
      } else {
        current = null;
      }
    }
  }

  @Override
  public int nextPosition() throws IOException {
    int pos = current.nextPosition();
    if (pos < 0) {
      throw new CorruptIndexException("position=" + pos + " is negative, field=\"" + field + " doc=" + doc,
                                      mergeState.fieldsProducers[upto].toString());
    } else if (pos > IndexWriter.MAX_POSITION) {
      throw new CorruptIndexException("position=" + pos + " is too large (> IndexWriter.MAX_POSITION=" + IndexWriter.MAX_POSITION + "), field=\"" + field + "\" doc=" + doc,
                                      mergeState.fieldsProducers[upto].toString());
    }
    return pos;
  }
  
  @Override
  public int startOffset() throws IOException {
    return current.startOffset();
  }
  
  @Override
  public int endOffset() throws IOException {
    return current.endOffset();
  }
  
  @Override
  public BytesRef getPayload() throws IOException {
    return current.getPayload();
  }

  @Override
  public long cost() {
    long cost = 0;
    for (EnumWithSlice enumWithSlice : subs) {
      cost += enumWithSlice.postingsEnum.cost();
    }
    return cost;
  }
}

