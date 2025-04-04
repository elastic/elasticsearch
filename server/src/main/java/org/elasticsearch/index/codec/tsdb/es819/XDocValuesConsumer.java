/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

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
package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.PackedInts;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Abstract API that consumes numeric, binary and sorted docvalues. Concrete implementations of this
 * actually do "something" with the docvalues (write it into the index in a specific format).
 *
 * <p>The lifecycle is:
 *
 * <ol>
 *   <li>DocValuesConsumer is created by {@link DocValuesFormat#fieldsConsumer(SegmentWriteState)}.
 *   <li>{@link #addNumericField}, {@link #addBinaryField}, {@link #addSortedField}, {@link
 *       #addSortedSetField}, or {@link #addSortedNumericField} are called for each Numeric, Binary,
 *       Sorted, SortedSet, or SortedNumeric docvalues field. The API is a "pull" rather than
 *       "push", and the implementation is free to iterate over the values multiple times ({@link
 *       Iterable#iterator()}).
 *   <li>After all fields are added, the consumer is {@link #close}d.
 * </ol>
 *
 * @lucene.experimental
 */
public abstract class XDocValuesConsumer implements Closeable {

  /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
  protected XDocValuesConsumer() {}

  /**
   * Writes numeric docvalues for a field.
   *
   * @param field field information
   * @param valuesProducer Numeric values to write.
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addNumericField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException;

  /**
   * Writes binary docvalues for a field.
   *
   * @param field field information
   * @param valuesProducer Binary values to write.
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException;

  /**
   * Writes pre-sorted binary docvalues for a field.
   *
   * @param field field information
   * @param valuesProducer produces the values and ordinals to write
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addSortedField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException;

  /**
   * Writes pre-sorted numeric docvalues for a field
   *
   * @param field field information
   * @param valuesProducer produces the values to write
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException;

  /**
   * Writes pre-sorted set docvalues for a field
   *
   * @param field field information
   * @param valuesProducer produces the values to write
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException;

  /**
   * Merges in the fields from the readers in <code>mergeState</code>. The default implementation
   * calls {@link #mergeNumericField}, {@link #mergeBinaryField}, {@link #mergeSortedField}, {@link
   * #mergeSortedSetField}, or {@link #mergeSortedNumericField} for each field, depending on its
   * type. Implementations can override this method for more sophisticated merging (bulk-byte
   * copying, etc).
   */
  public void merge(MergeState mergeState) throws IOException {
    for (DocValuesProducer docValuesProducer : mergeState.docValuesProducers) {
      if (docValuesProducer != null) {
        docValuesProducer.checkIntegrity();
      }
    }

    for (FieldInfo mergeFieldInfo : mergeState.mergeFieldInfos) {
      DocValuesType type = mergeFieldInfo.getDocValuesType();
      if (type != DocValuesType.NONE) {
        if (type == DocValuesType.NUMERIC) {
          mergeNumericField(mergeFieldInfo, mergeState);
        } else if (type == DocValuesType.BINARY) {
          mergeBinaryField(mergeFieldInfo, mergeState);
        } else if (type == DocValuesType.SORTED) {
          mergeSortedField(mergeFieldInfo, mergeState);
        } else if (type == DocValuesType.SORTED_SET) {
          mergeSortedSetField(mergeFieldInfo, mergeState);
        } else if (type == DocValuesType.SORTED_NUMERIC) {
          mergeSortedNumericField(mergeFieldInfo, mergeState);
        } else {
          throw new AssertionError("type=" + type);
        }
      }
    }
  }

  /** Tracks state of one numeric sub-reader that we are merging */
  private static class NumericDocValuesSub extends DocIDMerger.Sub {

    final NumericDocValues values;

    public NumericDocValuesSub(MergeState.DocMap docMap, NumericDocValues values) {
      super(docMap);
      this.values = values;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }

  /**
   * Merges the numeric docvalues from <code>MergeState</code>.
   *
   * <p>The default implementation calls {@link #addNumericField}, passing a DocValuesProducer that
   * merges and filters deleted documents on the fly.
   */
  public void mergeNumericField(final FieldInfo mergeFieldInfo, final MergeState mergeState)
      throws IOException {
    addNumericField(
        mergeFieldInfo,
        new EmptyDocValuesProducer() {
          @Override
          public NumericDocValues getNumeric(FieldInfo fieldInfo) throws IOException {
            if (fieldInfo != mergeFieldInfo) {
              throw new IllegalArgumentException("wrong fieldInfo");
            }

            List<NumericDocValuesSub> subs = new ArrayList<>();
            assert mergeState.docMaps.length == mergeState.docValuesProducers.length;
            for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
              NumericDocValues values = null;
              DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
              if (docValuesProducer != null) {
                FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
                if (readerFieldInfo != null
                    && readerFieldInfo.getDocValuesType() == DocValuesType.NUMERIC) {
                  values = docValuesProducer.getNumeric(readerFieldInfo);
                }
              }
              if (values != null) {
                subs.add(new NumericDocValuesSub(mergeState.docMaps[i], values));
              }
            }

            return mergeNumericValues(subs, mergeState.needsIndexSort);
          }
        });
  }

  private static NumericDocValues mergeNumericValues(
      List<NumericDocValuesSub> subs, boolean indexIsSorted) throws IOException {
    long cost = 0;
    for (NumericDocValuesSub sub : subs) {
      cost += sub.values.cost();
    }
    final long finalCost = cost;

    final DocIDMerger<NumericDocValuesSub> docIDMerger = DocIDMerger.of(subs, indexIsSorted);

    return new NumericDocValues() {
      private int docID = -1;
      private NumericDocValuesSub current;

      @Override
      public int docID() {
        return docID;
      }

      @Override
      public int nextDoc() throws IOException {
        current = docIDMerger.next();
        if (current == null) {
          docID = NO_MORE_DOCS;
        } else {
          docID = current.mappedDocID;
        }
        return docID;
      }

      @Override
      public int advance(int target) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public long cost() {
        return finalCost;
      }

      @Override
      public long longValue() throws IOException {
        return current.values.longValue();
      }
    };
  }

  /** Tracks state of one binary sub-reader that we are merging */
  private static class BinaryDocValuesSub extends DocIDMerger.Sub {

    final BinaryDocValues values;

    public BinaryDocValuesSub(MergeState.DocMap docMap, BinaryDocValues values) {
      super(docMap);
      this.values = values;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }

  /**
   * Merges the binary docvalues from <code>MergeState</code>.
   *
   * <p>The default implementation calls {@link #addBinaryField}, passing a DocValuesProducer that
   * merges and filters deleted documents on the fly.
   */
  public void mergeBinaryField(FieldInfo mergeFieldInfo, final MergeState mergeState)
      throws IOException {
    addBinaryField(
        mergeFieldInfo,
        new EmptyDocValuesProducer() {
          @Override
          public BinaryDocValues getBinary(FieldInfo fieldInfo) throws IOException {
            if (fieldInfo != mergeFieldInfo) {
              throw new IllegalArgumentException("wrong fieldInfo");
            }

            List<BinaryDocValuesSub> subs = new ArrayList<>();

            long cost = 0;
            for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
              BinaryDocValues values = null;
              DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
              if (docValuesProducer != null) {
                FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
                if (readerFieldInfo != null
                    && readerFieldInfo.getDocValuesType() == DocValuesType.BINARY) {
                  values = docValuesProducer.getBinary(readerFieldInfo);
                }
              }
              if (values != null) {
                cost += values.cost();
                subs.add(new BinaryDocValuesSub(mergeState.docMaps[i], values));
              }
            }

            final DocIDMerger<BinaryDocValuesSub> docIDMerger =
                DocIDMerger.of(subs, mergeState.needsIndexSort);
            final long finalCost = cost;

            return new BinaryDocValues() {
              private BinaryDocValuesSub current;
              private int docID = -1;

              @Override
              public int docID() {
                return docID;
              }

              @Override
              public int nextDoc() throws IOException {
                current = docIDMerger.next();
                if (current == null) {
                  docID = NO_MORE_DOCS;
                } else {
                  docID = current.mappedDocID;
                }
                return docID;
              }

              @Override
              public int advance(int target) throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public boolean advanceExact(int target) throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public long cost() {
                return finalCost;
              }

              @Override
              public BytesRef binaryValue() throws IOException {
                return current.values.binaryValue();
              }
            };
          }
        });
  }

  /** Tracks state of one sorted numeric sub-reader that we are merging */
  private static class SortedNumericDocValuesSub extends DocIDMerger.Sub {

    final SortedNumericDocValues values;

    public SortedNumericDocValuesSub(MergeState.DocMap docMap, SortedNumericDocValues values) {
      super(docMap);
      this.values = values;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }

  /**
   * Merges the sorted docvalues from <code>toMerge</code>.
   *
   * <p>The default implementation calls {@link #addSortedNumericField}, passing iterables that
   * filter deleted documents.
   */
  public void mergeSortedNumericField(FieldInfo mergeFieldInfo, final MergeState mergeState)
      throws IOException {

    addSortedNumericField(
        mergeFieldInfo,
        new EmptyDocValuesProducer() {
          @Override
          public SortedNumericDocValues getSortedNumeric(FieldInfo fieldInfo) throws IOException {
            if (fieldInfo != mergeFieldInfo) {
              throw new IllegalArgumentException("wrong FieldInfo");
            }

            // We must make new iterators + DocIDMerger for each iterator:
            List<SortedNumericDocValuesSub> subs = new ArrayList<>();
            long cost = 0;
            boolean allSingletons = true;
            for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
              DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
              SortedNumericDocValues values = null;
              if (docValuesProducer != null) {
                FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
                if (readerFieldInfo != null
                    && readerFieldInfo.getDocValuesType() == DocValuesType.SORTED_NUMERIC) {
                  values = docValuesProducer.getSortedNumeric(readerFieldInfo);
                }
              }
              if (values == null) {
                values = DocValues.emptySortedNumeric();
              }
              cost += values.cost();
              if (allSingletons && DocValues.unwrapSingleton(values) == null) {
                allSingletons = false;
              }
              subs.add(new SortedNumericDocValuesSub(mergeState.docMaps[i], values));
            }

            if (allSingletons) {
              // All subs are single-valued.
              // We specialize for that case since it makes it easier for codecs to optimize
              // for single-valued fields.
              List<NumericDocValuesSub> singleValuedSubs = new ArrayList<>();
              for (SortedNumericDocValuesSub sub : subs) {
                final NumericDocValues singleValuedValues = DocValues.unwrapSingleton(sub.values);
                assert singleValuedValues != null;
                singleValuedSubs.add(new NumericDocValuesSub(sub.docMap, singleValuedValues));
              }
              return DocValues.singleton(
                  mergeNumericValues(singleValuedSubs, mergeState.needsIndexSort));
            }

            final long finalCost = cost;

            final DocIDMerger<SortedNumericDocValuesSub> docIDMerger =
                DocIDMerger.of(subs, mergeState.needsIndexSort);

            return new SortedNumericDocValues() {

              private int docID = -1;
              private SortedNumericDocValuesSub currentSub;

              @Override
              public int docID() {
                return docID;
              }

              @Override
              public int nextDoc() throws IOException {
                currentSub = docIDMerger.next();
                if (currentSub == null) {
                  docID = NO_MORE_DOCS;
                } else {
                  docID = currentSub.mappedDocID;
                }

                return docID;
              }

              @Override
              public int advance(int target) throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public boolean advanceExact(int target) throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public int docValueCount() {
                return currentSub.values.docValueCount();
              }

              @Override
              public long cost() {
                return finalCost;
              }

              @Override
              public long nextValue() throws IOException {
                return currentSub.values.nextValue();
              }
            };
          }
        });
  }

  /**
   * A merged {@link TermsEnum}. This helps avoid relying on the default terms enum, which calls
   * {@link SortedDocValues#lookupOrd(int)} or {@link SortedSetDocValues#lookupOrd(long)} on every
   * call to {@link TermsEnum#next()}.
   */
  private static class MergedTermsEnum extends BaseTermsEnum {

    private final TermsEnum[] subs;
    private final OrdinalMap ordinalMap;
    private final long valueCount;
    private long ord = -1;
    private BytesRef term;

    MergedTermsEnum(OrdinalMap ordinalMap, TermsEnum[] subs) {
      this.ordinalMap = ordinalMap;
      this.subs = subs;
      this.valueCount = ordinalMap.getValueCount();
    }

    @Override
    public BytesRef term() throws IOException {
      return term;
    }

    @Override
    public long ord() throws IOException {
      return ord;
    }

    @Override
    public BytesRef next() throws IOException {
      if (++ord >= valueCount) {
        return null;
      }
      final int subNum = ordinalMap.getFirstSegmentNumber(ord);
      final TermsEnum sub = subs[subNum];
      final long subOrd = ordinalMap.getFirstSegmentOrd(ord);
      do {
        term = sub.next();
      } while (sub.ord() < subOrd);
      assert sub.ord() == subOrd;
      return term;
    }

    @Override
    public AttributeSource attributes() {
      throw new UnsupportedOperationException();
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void seekExact(long ord) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long totalTermFreq() throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public TermState termState() throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  /** Tracks state of one sorted sub-reader that we are merging */
  private static class SortedDocValuesSub extends DocIDMerger.Sub {

    final SortedDocValues values;
    final LongValues map;

    public SortedDocValuesSub(MergeState.DocMap docMap, SortedDocValues values, LongValues map) {
      super(docMap);
      this.values = values;
      this.map = map;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }
  }

  /**
   * Merges the sorted docvalues from <code>toMerge</code>.
   *
   * <p>The default implementation calls {@link #addSortedField}, passing an Iterable that merges
   * ordinals and values and filters deleted documents .
   */
  public void mergeSortedField(FieldInfo fieldInfo, final MergeState mergeState)
      throws IOException {
    List<SortedDocValues> toMerge = new ArrayList<>();
    for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
      SortedDocValues values = null;
      DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
      if (docValuesProducer != null) {
        FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(fieldInfo.name);
        if (readerFieldInfo != null && readerFieldInfo.getDocValuesType() == DocValuesType.SORTED) {
          values = docValuesProducer.getSorted(readerFieldInfo);
        }
      }
      if (values == null) {
        values = DocValues.emptySorted();
      }
      toMerge.add(values);
    }

    final int numReaders = toMerge.size();
    final SortedDocValues[] dvs = toMerge.toArray(new SortedDocValues[numReaders]);

    // step 1: iterate thru each sub and mark terms still in use
    TermsEnum[] liveTerms = new TermsEnum[dvs.length];
    long[] weights = new long[liveTerms.length];
    for (int sub = 0; sub < numReaders; sub++) {
      SortedDocValues dv = dvs[sub];
      Bits liveDocs = mergeState.liveDocs[sub];
      if (liveDocs == null) {
        liveTerms[sub] = dv.termsEnum();
        weights[sub] = dv.getValueCount();
      } else {
        LongBitSet bitset = new LongBitSet(dv.getValueCount());
        int docID;
        while ((docID = dv.nextDoc()) != NO_MORE_DOCS) {
          if (liveDocs.get(docID)) {
            int ord = dv.ordValue();
            if (ord >= 0) {
              bitset.set(ord);
            }
          }
        }
        liveTerms[sub] = new BitsFilteredTermsEnum(dv.termsEnum(), bitset);
        weights[sub] = bitset.cardinality();
      }
    }

    // step 2: create ordinal map (this conceptually does the "merging")
    final OrdinalMap map = OrdinalMap.build(null, liveTerms, weights, PackedInts.COMPACT);

    // step 3: add field
    addSortedField(
        fieldInfo,
        new EmptyDocValuesProducer() {
          @Override
          public SortedDocValues getSorted(FieldInfo fieldInfoIn) throws IOException {
            if (fieldInfoIn != fieldInfo) {
              throw new IllegalArgumentException("wrong FieldInfo");
            }

            // We must make new iterators + DocIDMerger for each iterator:

            List<SortedDocValuesSub> subs = new ArrayList<>();
            for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
              SortedDocValues values = null;
              DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
              if (docValuesProducer != null) {
                FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(fieldInfo.name);
                if (readerFieldInfo != null
                    && readerFieldInfo.getDocValuesType() == DocValuesType.SORTED) {
                  values = docValuesProducer.getSorted(readerFieldInfo);
                }
              }
              if (values == null) {
                values = DocValues.emptySorted();
              }

              subs.add(new SortedDocValuesSub(mergeState.docMaps[i], values, map.getGlobalOrds(i)));
            }

            return mergeSortedValues(subs, mergeState.needsIndexSort, map);
          }
        });
  }

  private static SortedDocValues mergeSortedValues(
      List<SortedDocValuesSub> subs, boolean indexIsSorted, OrdinalMap map) throws IOException {
    long cost = 0;
    for (SortedDocValuesSub sub : subs) {
      cost += sub.values.cost();
    }
    final long finalCost = cost;

    final DocIDMerger<SortedDocValuesSub> docIDMerger = DocIDMerger.of(subs, indexIsSorted);

    return new SortedDocValues() {
      private int docID = -1;
      private SortedDocValuesSub current;

      @Override
      public int docID() {
        return docID;
      }

      @Override
      public int nextDoc() throws IOException {
        current = docIDMerger.next();
        if (current == null) {
          docID = NO_MORE_DOCS;
        } else {
          docID = current.mappedDocID;
        }
        return docID;
      }

      @Override
      public int ordValue() throws IOException {
        int subOrd = current.values.ordValue();
        assert subOrd != -1;
        return (int) current.map.get(subOrd);
      }

      @Override
      public int advance(int target) {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean advanceExact(int target) throws IOException {
        throw new UnsupportedOperationException();
      }

      @Override
      public long cost() {
        return finalCost;
      }

      @Override
      public int getValueCount() {
        return (int) map.getValueCount();
      }

      @Override
      public BytesRef lookupOrd(int ord) throws IOException {
        int segmentNumber = map.getFirstSegmentNumber(ord);
        int segmentOrd = (int) map.getFirstSegmentOrd(ord);
        return subs.get(segmentNumber).values.lookupOrd(segmentOrd);
      }

      @Override
      public TermsEnum termsEnum() throws IOException {
        TermsEnum[] termsEnurmSubs = new TermsEnum[subs.size()];
        for (int sub = 0; sub < termsEnurmSubs.length; ++sub) {
          termsEnurmSubs[sub] = subs.get(sub).values.termsEnum();
        }
        return new MergedTermsEnum(map, termsEnurmSubs);
      }
    };
  }

  /** Tracks state of one sorted set sub-reader that we are merging */
  private static class SortedSetDocValuesSub extends DocIDMerger.Sub {

    final SortedSetDocValues values;
    final LongValues map;

    public SortedSetDocValuesSub(
        MergeState.DocMap docMap, SortedSetDocValues values, LongValues map) {
      super(docMap);
      this.values = values;
      this.map = map;
      assert values.docID() == -1;
    }

    @Override
    public int nextDoc() throws IOException {
      return values.nextDoc();
    }

    @Override
    public String toString() {
      return "SortedSetDocValuesSub(mappedDocID=" + mappedDocID + " values=" + values + ")";
    }
  }

  /**
   * Merges the sortedset docvalues from <code>toMerge</code>.
   *
   * <p>The default implementation calls {@link #addSortedSetField}, passing an Iterable that merges
   * ordinals and values and filters deleted documents .
   */
  public void mergeSortedSetField(FieldInfo mergeFieldInfo, final MergeState mergeState)
      throws IOException {

    List<SortedSetDocValues> toMerge = new ArrayList<>();
    for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
      SortedSetDocValues values = null;
      DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
      if (docValuesProducer != null) {
        FieldInfo fieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
        if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.SORTED_SET) {
          values = docValuesProducer.getSortedSet(fieldInfo);
        }
      }
      if (values == null) {
        values = DocValues.emptySortedSet();
      }
      toMerge.add(values);
    }

    // step 1: iterate thru each sub and mark terms still in use
    TermsEnum[] liveTerms = new TermsEnum[toMerge.size()];
    long[] weights = new long[liveTerms.length];
    for (int sub = 0; sub < liveTerms.length; sub++) {
      SortedSetDocValues dv = toMerge.get(sub);
      Bits liveDocs = mergeState.liveDocs[sub];
      if (liveDocs == null) {
        liveTerms[sub] = dv.termsEnum();
        weights[sub] = dv.getValueCount();
      } else {
        LongBitSet bitset = new LongBitSet(dv.getValueCount());
        int docID;
        while ((docID = dv.nextDoc()) != NO_MORE_DOCS) {
          if (liveDocs.get(docID)) {
            for (int i = 0; i < dv.docValueCount(); i++) {
              bitset.set(dv.nextOrd());
            }
          }
        }
        liveTerms[sub] = new BitsFilteredTermsEnum(dv.termsEnum(), bitset);
        weights[sub] = bitset.cardinality();
      }
    }

    // step 2: create ordinal map (this conceptually does the "merging")
    final OrdinalMap map = OrdinalMap.build(null, liveTerms, weights, PackedInts.COMPACT);

    // step 3: add field
    addSortedSetField(
        mergeFieldInfo,
        new EmptyDocValuesProducer() {
          @Override
          public SortedSetDocValues getSortedSet(FieldInfo fieldInfo) throws IOException {
            if (fieldInfo != mergeFieldInfo) {
              throw new IllegalArgumentException("wrong FieldInfo");
            }

            // We must make new iterators + DocIDMerger for each iterator:
            List<SortedSetDocValuesSub> subs = new ArrayList<>();

            long cost = 0;
            boolean allSingletons = true;

            for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
              SortedSetDocValues values = null;
              DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
              if (docValuesProducer != null) {
                FieldInfo readerFieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
                if (readerFieldInfo != null
                    && readerFieldInfo.getDocValuesType() == DocValuesType.SORTED_SET) {
                  values = docValuesProducer.getSortedSet(readerFieldInfo);
                }
              }
              if (values == null) {
                values = DocValues.emptySortedSet();
              }
              cost += values.cost();
              if (allSingletons && DocValues.unwrapSingleton(values) == null) {
                allSingletons = false;
              }
              subs.add(
                  new SortedSetDocValuesSub(mergeState.docMaps[i], values, map.getGlobalOrds(i)));
            }

            if (allSingletons) {
              // All subs are single-valued.
              // We specialize for that case since it makes it easier for codecs to optimize
              // for single-valued fields.
              List<SortedDocValuesSub> singleValuedSubs = new ArrayList<>();
              for (SortedSetDocValuesSub sub : subs) {
                final SortedDocValues singleValuedValues = DocValues.unwrapSingleton(sub.values);
                assert singleValuedValues != null;
                singleValuedSubs.add(
                    new SortedDocValuesSub(sub.docMap, singleValuedValues, sub.map));
              }
              return DocValues.singleton(
                  mergeSortedValues(singleValuedSubs, mergeState.needsIndexSort, map));
            }

            final DocIDMerger<SortedSetDocValuesSub> docIDMerger =
                DocIDMerger.of(subs, mergeState.needsIndexSort);

            final long finalCost = cost;

            return new SortedSetDocValues() {
              private int docID = -1;
              private SortedSetDocValuesSub currentSub;

              @Override
              public int docID() {
                return docID;
              }

              @Override
              public int nextDoc() throws IOException {
                currentSub = docIDMerger.next();
                if (currentSub == null) {
                  docID = NO_MORE_DOCS;
                } else {
                  docID = currentSub.mappedDocID;
                }

                return docID;
              }

              @Override
              public int advance(int target) throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public boolean advanceExact(int target) throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public long nextOrd() throws IOException {
                long subOrd = currentSub.values.nextOrd();
                return currentSub.map.get(subOrd);
              }

              @Override
              public int docValueCount() {
                return currentSub.values.docValueCount();
              }

              @Override
              public long cost() {
                return finalCost;
              }

              @Override
              public BytesRef lookupOrd(long ord) throws IOException {
                int segmentNumber = map.getFirstSegmentNumber(ord);
                long segmentOrd = map.getFirstSegmentOrd(ord);
                return toMerge.get(segmentNumber).lookupOrd(segmentOrd);
              }

              @Override
              public long getValueCount() {
                return map.getValueCount();
              }

              @Override
              public TermsEnum termsEnum() throws IOException {
                TermsEnum[] subs = new TermsEnum[toMerge.size()];
                for (int sub = 0; sub < subs.length; ++sub) {
                  subs[sub] = toMerge.get(sub).termsEnum();
                }
                return new MergedTermsEnum(map, subs);
              }
            };
          }
        });
  }

  // TODO: seek-by-ord to nextSetBit
  static class BitsFilteredTermsEnum extends FilteredTermsEnum {
    final LongBitSet liveTerms;

    BitsFilteredTermsEnum(TermsEnum in, LongBitSet liveTerms) {
      super(in, false); // <-- not passing false here wasted about 3 hours of my time!!!!!!!!!!!!!
      assert liveTerms != null;
      this.liveTerms = liveTerms;
    }

    @Override
    protected AcceptStatus accept(BytesRef term) throws IOException {
      if (liveTerms.get(ord())) {
        return AcceptStatus.YES;
      } else {
        return AcceptStatus.NO;
      }
    }
  }

  /** Helper: returns true if the given docToValue count contains only at most one value */
  public static boolean isSingleValued(Iterable<Number> docToValueCount) {
    for (Number count : docToValueCount) {
      if (count.longValue() > 1) {
        return false;
      }
    }
    return true;
  }

  /** Helper: returns single-valued view, using {@code missingValue} when count is zero */
  public static Iterable<Number> singletonView(
      final Iterable<Number> docToValueCount,
      final Iterable<Number> values,
      final Number missingValue) {
    assert isSingleValued(docToValueCount);
    return new Iterable<Number>() {

      @Override
      public Iterator<Number> iterator() {
        final Iterator<Number> countIterator = docToValueCount.iterator();
        final Iterator<Number> valuesIterator = values.iterator();
        return new Iterator<Number>() {

          @Override
          public boolean hasNext() {
            return countIterator.hasNext();
          }

          @Override
          public Number next() {
            int count = countIterator.next().intValue();
            if (count == 0) {
              return missingValue;
            } else {
              return valuesIterator.next();
            }
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
