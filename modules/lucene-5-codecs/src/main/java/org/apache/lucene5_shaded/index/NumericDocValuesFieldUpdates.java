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

import org.apache.lucene5_shaded.document.NumericDocValuesField;
import org.apache.lucene5_shaded.search.DocIdSetIterator;
import org.apache.lucene5_shaded.util.InPlaceMergeSorter;
import org.apache.lucene5_shaded.util.packed.PackedInts;
import org.apache.lucene5_shaded.util.packed.PagedGrowableWriter;
import org.apache.lucene5_shaded.util.packed.PagedMutable;


/**
 * A {@link DocValuesFieldUpdates} which holds updates of documents, of a single
 * {@link NumericDocValuesField}.
 * 
 * @lucene.experimental
 */
class NumericDocValuesFieldUpdates extends DocValuesFieldUpdates {
  
  final static class Iterator extends DocValuesFieldUpdates.Iterator {
    private final int size;
    private final PagedGrowableWriter values;
    private final PagedMutable docs;
    private long idx = 0; // long so we don't overflow if size == Integer.MAX_VALUE
    private int doc = -1;
    private Long value = null;
    
    Iterator(int size, PagedGrowableWriter values, PagedMutable docs) {
      this.size = size;
      this.values = values;
      this.docs = docs;
    }
    
    @Override
    Long value() {
      return value;
    }
    
    @Override
    int nextDoc() {
      if (idx >= size) {
        value = null;
        return doc = DocIdSetIterator.NO_MORE_DOCS;
      }
      doc = (int) docs.get(idx);
      ++idx;
      while (idx < size && docs.get(idx) == doc) {
        ++idx;
      }
      // idx points to the "next" element
      value = Long.valueOf(values.get(idx - 1));
      return doc;
    }
    
    @Override
    int doc() {
      return doc;
    }
    
    @Override
    void reset() {
      doc = -1;
      value = null;
      idx = 0;
    }
  }

  private final int bitsPerValue;
  private PagedMutable docs;
  private PagedGrowableWriter values;
  private int size;
  
  public NumericDocValuesFieldUpdates(String field, int maxDoc) {
    super(field, DocValuesType.NUMERIC);
    bitsPerValue = PackedInts.bitsRequired(maxDoc - 1);
    docs = new PagedMutable(1, PAGE_SIZE, bitsPerValue, PackedInts.COMPACT);
    values = new PagedGrowableWriter(1, PAGE_SIZE, 1, PackedInts.FAST);
    size = 0;
  }
  
  @Override
  public void add(int doc, Object value) {
    // TODO: if the Sorter interface changes to take long indexes, we can remove that limitation
    if (size == Integer.MAX_VALUE) {
      throw new IllegalStateException("cannot support more than Integer.MAX_VALUE doc/value entries");
    }

    Long val = (Long) value;
    
    // grow the structures to have room for more elements
    if (docs.size() == size) {
      docs = docs.grow(size + 1);
      values = values.grow(size + 1);
    }
    
    docs.set(size, doc);
    values.set(size, val.longValue());
    ++size;
  }
  
  @Override
  public Iterator iterator() {
    final PagedMutable docs = this.docs;
    final PagedGrowableWriter values = this.values;
    new InPlaceMergeSorter() {
      @Override
      protected void swap(int i, int j) {
        long tmpDoc = docs.get(j);
        docs.set(j, docs.get(i));
        docs.set(i, tmpDoc);
        
        long tmpVal = values.get(j);
        values.set(j, values.get(i));
        values.set(i, tmpVal);
      }
      
      @Override
      protected int compare(int i, int j) {
        int x = (int) docs.get(i);
        int y = (int) docs.get(j);
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
      }
    }.sort(0, size);
    
    return new Iterator(size, values, docs);
  }
  
  @Override
  public void merge(DocValuesFieldUpdates other) {
    assert other instanceof NumericDocValuesFieldUpdates;
    NumericDocValuesFieldUpdates otherUpdates = (NumericDocValuesFieldUpdates) other;
    if (otherUpdates.size > Integer.MAX_VALUE - size) {
      throw new IllegalStateException(
          "cannot support more than Integer.MAX_VALUE doc/value entries; size="
              + size + " other.size=" + otherUpdates.size);
    }
    docs = docs.grow(size + otherUpdates.size);
    values = values.grow(size + otherUpdates.size);
    for (int i = 0; i < otherUpdates.size; i++) {
      int doc = (int) otherUpdates.docs.get(i);
      docs.set(size, doc);
      values.set(size, otherUpdates.values.get(i));
      ++size;
    }
  }

  @Override
  public boolean any() {
    return size > 0;
  }

  @Override
  public long ramBytesPerDoc() {
    long bytesPerDoc = (long) Math.ceil((double) (bitsPerValue) / 8);
    final int capacity = estimateCapacity(size);
    bytesPerDoc += (long) Math.ceil((double) values.ramBytesUsed() / capacity); // values
    return bytesPerDoc;
  }
  
}
