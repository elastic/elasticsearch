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
package org.apache.lucene5_shaded.codecs;


import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.lucene5_shaded.index.BinaryDocValues;
import org.apache.lucene5_shaded.index.FieldInfo;
import org.apache.lucene5_shaded.index.FilteredTermsEnum;
import org.apache.lucene5_shaded.index.MergeState;
import org.apache.lucene5_shaded.index.DocValuesType;
import org.apache.lucene5_shaded.index.MultiDocValues.OrdinalMap;
import org.apache.lucene5_shaded.index.DocValues;
import org.apache.lucene5_shaded.index.NumericDocValues;
import org.apache.lucene5_shaded.index.SegmentWriteState; // javadocs
import org.apache.lucene5_shaded.index.SortedDocValues;
import org.apache.lucene5_shaded.index.SortedNumericDocValues;
import org.apache.lucene5_shaded.index.SortedSetDocValues;
import org.apache.lucene5_shaded.index.TermsEnum;
import org.apache.lucene5_shaded.util.ArrayUtil;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.LongBitSet;
import org.apache.lucene5_shaded.util.LongValues;
import org.apache.lucene5_shaded.util.packed.PackedInts;

/** 
 * Abstract API that consumes numeric, binary and
 * sorted docvalues.  Concrete implementations of this
 * actually do "something" with the docvalues (write it into
 * the index in a specific format).
 * <p>
 * The lifecycle is:
 * <ol>
 *   <li>DocValuesConsumer is created by 
 *       {@link NormsFormat#normsConsumer(SegmentWriteState)}.
 *   <li>{@link #addNumericField}, {@link #addBinaryField},
 *       {@link #addSortedField}, {@link #addSortedSetField},
 *       or {@link #addSortedNumericField} are called for each Numeric,
 *       Binary, Sorted, SortedSet, or SortedNumeric docvalues field. 
 *       The API is a "pull" rather than "push", and the implementation 
 *       is free to iterate over the values multiple times 
 *       ({@link Iterable#iterator()}).
 *   <li>After all fields are added, the consumer is {@link #close}d.
 * </ol>
 *
 * @lucene.experimental
 */
public abstract class DocValuesConsumer implements Closeable {
  
  /** Sole constructor. (For invocation by subclass 
   *  constructors, typically implicit.) */
  protected DocValuesConsumer() {}

  /**
   * Writes numeric docvalues for a field.
   * @param field field information
   * @param values Iterable of numeric values (one for each document). {@code null} indicates
   *               a missing value.
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addNumericField(FieldInfo field, Iterable<Number> values) throws IOException;    

  /**
   * Writes binary docvalues for a field.
   * @param field field information
   * @param values Iterable of binary values (one for each document). {@code null} indicates
   *               a missing value.
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addBinaryField(FieldInfo field, Iterable<BytesRef> values) throws IOException;

  /**
   * Writes pre-sorted binary docvalues for a field.
   * @param field field information
   * @param values Iterable of binary values in sorted order (deduplicated).
   * @param docToOrd Iterable of ordinals (one for each document). {@code -1} indicates
   *                 a missing value.
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addSortedField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrd) throws IOException;
  
  /**
   * Writes pre-sorted numeric docvalues for a field
   * @param field field information
   * @param docToValueCount Iterable of the number of values for each document. A zero
   *                        count indicates a missing value.
   * @param values Iterable of numeric values in sorted order (not deduplicated).
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addSortedNumericField(FieldInfo field, Iterable<Number> docToValueCount, Iterable<Number> values) throws IOException;

  /**
   * Writes pre-sorted set docvalues for a field
   * @param field field information
   * @param values Iterable of binary values in sorted order (deduplicated).
   * @param docToOrdCount Iterable of the number of values for each document. A zero ordinal
   *                      count indicates a missing value.
   * @param ords Iterable of ordinal occurrences (docToOrdCount*maxDoc total).
   * @throws IOException if an I/O error occurred.
   */
  public abstract void addSortedSetField(FieldInfo field, Iterable<BytesRef> values, Iterable<Number> docToOrdCount, Iterable<Number> ords) throws IOException;
  
  /** Merges in the fields from the readers in 
   *  <code>mergeState</code>. The default implementation 
   *  calls {@link #mergeNumericField}, {@link #mergeBinaryField},
   *  {@link #mergeSortedField}, {@link #mergeSortedSetField},
   *  or {@link #mergeSortedNumericField} for each field,
   *  depending on its type.
   *  Implementations can override this method 
   *  for more sophisticated merging (bulk-byte copying, etc). */
  public void merge(MergeState mergeState) throws IOException {
    for(DocValuesProducer docValuesProducer : mergeState.docValuesProducers) {
      if (docValuesProducer != null) {
        docValuesProducer.checkIntegrity();
      }
    }

    for (FieldInfo mergeFieldInfo : mergeState.mergeFieldInfos) {
      DocValuesType type = mergeFieldInfo.getDocValuesType();
      if (type != DocValuesType.NONE) {
        if (type == DocValuesType.NUMERIC) {
          List<NumericDocValues> toMerge = new ArrayList<>();
          List<Bits> docsWithField = new ArrayList<>();
          for (int i=0;i<mergeState.docValuesProducers.length;i++) {
            NumericDocValues values = null;
            Bits bits = null;
            DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
            if (docValuesProducer != null) {
              FieldInfo fieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
              if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.NUMERIC) {
                values = docValuesProducer.getNumeric(fieldInfo);
                bits = docValuesProducer.getDocsWithField(fieldInfo);
              }
            }
            if (values == null) {
              values = DocValues.emptyNumeric();
              bits = new Bits.MatchNoBits(mergeState.maxDocs[i]);
            }
            toMerge.add(values);
            docsWithField.add(bits);
          }
          mergeNumericField(mergeFieldInfo, mergeState, toMerge, docsWithField);
        } else if (type == DocValuesType.BINARY) {
          List<BinaryDocValues> toMerge = new ArrayList<>();
          List<Bits> docsWithField = new ArrayList<>();
          for (int i=0;i<mergeState.docValuesProducers.length;i++) {
            BinaryDocValues values = null;
            Bits bits = null;
            DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
            if (docValuesProducer != null) {
              FieldInfo fieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
              if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.BINARY) {
                values = docValuesProducer.getBinary(fieldInfo);
                bits = docValuesProducer.getDocsWithField(fieldInfo);
              }
            }
            if (values == null) {
              values = DocValues.emptyBinary();
              bits = new Bits.MatchNoBits(mergeState.maxDocs[i]);
            }
            toMerge.add(values);
            docsWithField.add(bits);
          }
          mergeBinaryField(mergeFieldInfo, mergeState, toMerge, docsWithField);
        } else if (type == DocValuesType.SORTED) {
          List<SortedDocValues> toMerge = new ArrayList<>();
          for (int i=0;i<mergeState.docValuesProducers.length;i++) {
            SortedDocValues values = null;
            DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
            if (docValuesProducer != null) {
              FieldInfo fieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
              if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.SORTED) {
                values = docValuesProducer.getSorted(fieldInfo);
              }
            }
            if (values == null) {
              values = DocValues.emptySorted();
            }
            toMerge.add(values);
          }
          mergeSortedField(mergeFieldInfo, mergeState, toMerge);
        } else if (type == DocValuesType.SORTED_SET) {
          List<SortedSetDocValues> toMerge = new ArrayList<>();
          for (int i=0;i<mergeState.docValuesProducers.length;i++) {
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
          mergeSortedSetField(mergeFieldInfo, mergeState, toMerge);
        } else if (type == DocValuesType.SORTED_NUMERIC) {
          List<SortedNumericDocValues> toMerge = new ArrayList<>();
          for (int i=0;i<mergeState.docValuesProducers.length;i++) {
            SortedNumericDocValues values = null;
            DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
            if (docValuesProducer != null) {
              FieldInfo fieldInfo = mergeState.fieldInfos[i].fieldInfo(mergeFieldInfo.name);
              if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.SORTED_NUMERIC) {
                values = docValuesProducer.getSortedNumeric(fieldInfo);
              }
            }
            if (values == null) {
              values = DocValues.emptySortedNumeric(mergeState.maxDocs[i]);
            }
            toMerge.add(values);
          }
          mergeSortedNumericField(mergeFieldInfo, mergeState, toMerge);
        } else {
          throw new AssertionError("type=" + type);
        }
      }
    }
  }
  
  /**
   * Merges the numeric docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addNumericField}, passing
   * an Iterable that merges and filters deleted documents on the fly.
   */
  public void mergeNumericField(final FieldInfo fieldInfo, final MergeState mergeState, final List<NumericDocValues> toMerge, final List<Bits> docsWithField) throws IOException {

    addNumericField(fieldInfo,
                    new Iterable<Number>() {
                      @Override
                      public Iterator<Number> iterator() {
                        return new Iterator<Number>() {
                          int readerUpto = -1;
                          int docIDUpto;
                          long nextValue;
                          boolean nextHasValue;
                          int currentMaxDoc;
                          NumericDocValues currentValues;
                          Bits currentLiveDocs;
                          Bits currentDocsWithField;
                          boolean nextIsSet;

                          @Override
                          public boolean hasNext() {
                            return nextIsSet || setNext();
                          }

                          @Override
                          public void remove() {
                            throw new UnsupportedOperationException();
                          }

                          @Override
                          public Number next() {
                            if (!hasNext()) {
                              throw new NoSuchElementException();
                            }
                            assert nextIsSet;
                            nextIsSet = false;
                            return nextHasValue ? nextValue : null;
                          }

                          private boolean setNext() {
                            while (true) {
                              if (readerUpto == toMerge.size()) {
                                return false;
                              }

                              if (docIDUpto == currentMaxDoc) {
                                readerUpto++;
                                if (readerUpto < toMerge.size()) {
                                  currentValues = toMerge.get(readerUpto);
                                  currentDocsWithField = docsWithField.get(readerUpto);
                                  currentLiveDocs = mergeState.liveDocs[readerUpto];
                                  currentMaxDoc = mergeState.maxDocs[readerUpto];
                                }
                                docIDUpto = 0;
                                continue;
                              }

                              if (currentLiveDocs == null || currentLiveDocs.get(docIDUpto)) {
                                nextIsSet = true;
                                nextValue = currentValues.get(docIDUpto);
                                if (nextValue == 0 && currentDocsWithField.get(docIDUpto) == false) {
                                  nextHasValue = false;
                                } else {
                                  nextHasValue = true;
                                }
                                docIDUpto++;
                                return true;
                              }

                              docIDUpto++;
                            }
                          }
                        };
                      }
                    });
  }
  
  /**
   * Merges the binary docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addBinaryField}, passing
   * an Iterable that merges and filters deleted documents on the fly.
   */
  public void mergeBinaryField(FieldInfo fieldInfo, final MergeState mergeState, final List<BinaryDocValues> toMerge, final List<Bits> docsWithField) throws IOException {

    addBinaryField(fieldInfo,
                   new Iterable<BytesRef>() {
                     @Override
                     public Iterator<BytesRef> iterator() {
                       return new Iterator<BytesRef>() {
                         int readerUpto = -1;
                         int docIDUpto;
                         BytesRef nextValue;
                         BytesRef nextPointer; // points to null if missing, or nextValue
                         int currentMaxDoc;
                         BinaryDocValues currentValues;
                         Bits currentLiveDocs;
                         Bits currentDocsWithField;
                         boolean nextIsSet;

                         @Override
                         public boolean hasNext() {
                           return nextIsSet || setNext();
                         }

                         @Override
                         public void remove() {
                           throw new UnsupportedOperationException();
                         }

                         @Override
                         public BytesRef next() {
                           if (!hasNext()) {
                             throw new NoSuchElementException();
                           }
                           assert nextIsSet;
                           nextIsSet = false;
                           return nextPointer;
                         }

                         private boolean setNext() {
                           while (true) {
                             if (readerUpto == toMerge.size()) {
                               return false;
                             }

                             if (docIDUpto == currentMaxDoc) {
                               readerUpto++;
                               if (readerUpto < toMerge.size()) {
                                 currentValues = toMerge.get(readerUpto);
                                 currentDocsWithField = docsWithField.get(readerUpto);
                                 currentLiveDocs = mergeState.liveDocs[readerUpto];
                                 currentMaxDoc = mergeState.maxDocs[readerUpto];
                               }
                               docIDUpto = 0;
                               continue;
                             }

                             if (currentLiveDocs == null || currentLiveDocs.get(docIDUpto)) {
                               nextIsSet = true;
                               if (currentDocsWithField.get(docIDUpto)) {
                                 nextValue = currentValues.get(docIDUpto);
                                 nextPointer = nextValue;
                               } else {
                                 nextPointer = null;
                               }
                               docIDUpto++;
                               return true;
                             }

                             docIDUpto++;
                           }
                         }
                       };
                     }
                   });
  }

  /**
   * Merges the sorted docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addSortedNumericField}, passing
   * iterables that filter deleted documents.
   */
  public void mergeSortedNumericField(FieldInfo fieldInfo, final MergeState mergeState, List<SortedNumericDocValues> toMerge) throws IOException {
    final int numReaders = toMerge.size();
    final SortedNumericDocValues dvs[] = toMerge.toArray(new SortedNumericDocValues[numReaders]);
    
    // step 3: add field
    addSortedNumericField(fieldInfo,
        // doc -> value count
        new Iterable<Number>() {
          @Override
          public Iterator<Number> iterator() {
            return new Iterator<Number>() {
              int readerUpto = -1;
              int docIDUpto;
              int nextValue;
              int currentMaxDoc;
              Bits currentLiveDocs;
              boolean nextIsSet;

              @Override
              public boolean hasNext() {
                return nextIsSet || setNext();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public Number next() {
                if (!hasNext()) {
                  throw new NoSuchElementException();
                }
                assert nextIsSet;
                nextIsSet = false;
                return nextValue;
              }

              private boolean setNext() {
                while (true) {
                  if (readerUpto == numReaders) {
                    return false;
                  }

                  if (docIDUpto == currentMaxDoc) {
                    readerUpto++;
                    if (readerUpto < numReaders) {
                      currentLiveDocs = mergeState.liveDocs[readerUpto];
                      currentMaxDoc = mergeState.maxDocs[readerUpto];
                    }
                    docIDUpto = 0;
                    continue;
                  }

                  if (currentLiveDocs == null || currentLiveDocs.get(docIDUpto)) {
                    nextIsSet = true;
                    SortedNumericDocValues dv = dvs[readerUpto];
                    dv.setDocument(docIDUpto);
                    nextValue = dv.count();
                    docIDUpto++;
                    return true;
                  }

                  docIDUpto++;
                }
              }
            };
          }
        },
        // values
        new Iterable<Number>() {
          @Override
          public Iterator<Number> iterator() {
            return new Iterator<Number>() {
              int readerUpto = -1;
              int docIDUpto;
              long nextValue;
              int currentMaxDoc;
              Bits currentLiveDocs;
              boolean nextIsSet;
              int valueUpto;
              int valueLength;

              @Override
              public boolean hasNext() {
                return nextIsSet || setNext();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public Number next() {
                if (!hasNext()) {
                  throw new NoSuchElementException();
                }
                assert nextIsSet;
                nextIsSet = false;
                return nextValue;
              }

              private boolean setNext() {
                while (true) {
                  if (readerUpto == numReaders) {
                    return false;
                  }
                  
                  if (valueUpto < valueLength) {
                    nextValue = dvs[readerUpto].valueAt(valueUpto);
                    valueUpto++;
                    nextIsSet = true;
                    return true;
                  }

                  if (docIDUpto == currentMaxDoc) {
                    readerUpto++;
                    if (readerUpto < numReaders) {
                      currentLiveDocs = mergeState.liveDocs[readerUpto];
                      currentMaxDoc = mergeState.maxDocs[readerUpto];
                    }
                    docIDUpto = 0;
                    continue;
                  }
                  
                  if (currentLiveDocs == null || currentLiveDocs.get(docIDUpto)) {
                    assert docIDUpto < currentMaxDoc;
                    SortedNumericDocValues dv = dvs[readerUpto];
                    dv.setDocument(docIDUpto);
                    valueUpto = 0;
                    valueLength = dv.count();
                    docIDUpto++;
                    continue;
                  }

                  docIDUpto++;
                }
              }
            };
          }
        }
     );
  }

  /**
   * Merges the sorted docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addSortedField}, passing
   * an Iterable that merges ordinals and values and filters deleted documents .
   */
  public void mergeSortedField(FieldInfo fieldInfo, final MergeState mergeState, List<SortedDocValues> toMerge) throws IOException {
    final int numReaders = toMerge.size();
    final SortedDocValues dvs[] = toMerge.toArray(new SortedDocValues[numReaders]);
    
    // step 1: iterate thru each sub and mark terms still in use
    TermsEnum liveTerms[] = new TermsEnum[dvs.length];
    long[] weights = new long[liveTerms.length];
    for (int sub=0;sub<numReaders;sub++) {
      SortedDocValues dv = dvs[sub];
      Bits liveDocs = mergeState.liveDocs[sub];
      int maxDoc = mergeState.maxDocs[sub];
      if (liveDocs == null) {
        liveTerms[sub] = dv.termsEnum();
        weights[sub] = dv.getValueCount();
      } else {
        LongBitSet bitset = new LongBitSet(dv.getValueCount());
        for (int i = 0; i < maxDoc; i++) {
          if (liveDocs.get(i)) {
            int ord = dv.getOrd(i);
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
    final OrdinalMap map = OrdinalMap.build(this, liveTerms, weights, PackedInts.COMPACT);
    
    // step 3: add field
    addSortedField(fieldInfo,
        // ord -> value
        new Iterable<BytesRef>() {
          @Override
          public Iterator<BytesRef> iterator() {
            return new Iterator<BytesRef>() {
              int currentOrd;

              @Override
              public boolean hasNext() {
                return currentOrd < map.getValueCount();
              }

              @Override
              public BytesRef next() {
                if (!hasNext()) {
                  throw new NoSuchElementException();
                }
                int segmentNumber = map.getFirstSegmentNumber(currentOrd);
                int segmentOrd = (int)map.getFirstSegmentOrd(currentOrd);
                final BytesRef term = dvs[segmentNumber].lookupOrd(segmentOrd);
                currentOrd++;
                return term;
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };
          }
        },
        // doc -> ord
        new Iterable<Number>() {
          @Override
          public Iterator<Number> iterator() {
            return new Iterator<Number>() {
              int readerUpto = -1;
              int docIDUpto;
              int nextValue;
              int currentMaxDoc;
              Bits currentLiveDocs;
              LongValues currentMap;
              boolean nextIsSet;

              @Override
              public boolean hasNext() {
                return nextIsSet || setNext();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public Number next() {
                if (!hasNext()) {
                  throw new NoSuchElementException();
                }
                assert nextIsSet;
                nextIsSet = false;
                // TODO make a mutable number
                return nextValue;
              }

              private boolean setNext() {
                while (true) {
                  if (readerUpto == numReaders) {
                    return false;
                  }

                  if (docIDUpto == currentMaxDoc) {
                    readerUpto++;
                    if (readerUpto < numReaders) {
                      currentMap = map.getGlobalOrds(readerUpto);
                      currentLiveDocs = mergeState.liveDocs[readerUpto];
                      currentMaxDoc = mergeState.maxDocs[readerUpto];
                    }
                    docIDUpto = 0;
                    continue;
                  }

                  if (currentLiveDocs == null || currentLiveDocs.get(docIDUpto)) {
                    nextIsSet = true;
                    int segOrd = dvs[readerUpto].getOrd(docIDUpto);
                    nextValue = segOrd == -1 ? -1 : (int) currentMap.get(segOrd);
                    docIDUpto++;
                    return true;
                  }

                  docIDUpto++;
                }
              }
            };
          }
        }
    );
  }
  
  /**
   * Merges the sortedset docvalues from <code>toMerge</code>.
   * <p>
   * The default implementation calls {@link #addSortedSetField}, passing
   * an Iterable that merges ordinals and values and filters deleted documents .
   */
  public void mergeSortedSetField(FieldInfo fieldInfo, final MergeState mergeState, List<SortedSetDocValues> toMerge) throws IOException {
    final SortedSetDocValues dvs[] = toMerge.toArray(new SortedSetDocValues[toMerge.size()]);
    final int numReaders = mergeState.maxDocs.length;

    // step 1: iterate thru each sub and mark terms still in use
    TermsEnum liveTerms[] = new TermsEnum[dvs.length];
    long[] weights = new long[liveTerms.length];
    for (int sub = 0; sub < liveTerms.length; sub++) {
      SortedSetDocValues dv = dvs[sub];
      Bits liveDocs = mergeState.liveDocs[sub];
      int maxDoc = mergeState.maxDocs[sub];
      if (liveDocs == null) {
        liveTerms[sub] = dv.termsEnum();
        weights[sub] = dv.getValueCount();
      } else {
        LongBitSet bitset = new LongBitSet(dv.getValueCount());
        for (int i = 0; i < maxDoc; i++) {
          if (liveDocs.get(i)) {
            dv.setDocument(i);
            long ord;
            while ((ord = dv.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
              bitset.set(ord);
            }
          }
        }
        liveTerms[sub] = new BitsFilteredTermsEnum(dv.termsEnum(), bitset);
        weights[sub] = bitset.cardinality();
      }
    }
    
    // step 2: create ordinal map (this conceptually does the "merging")
    final OrdinalMap map = OrdinalMap.build(this, liveTerms, weights, PackedInts.COMPACT);
    
    // step 3: add field
    addSortedSetField(fieldInfo,
        // ord -> value
        new Iterable<BytesRef>() {
          @Override
          public Iterator<BytesRef> iterator() {
            return new Iterator<BytesRef>() {
              long currentOrd;

              @Override
              public boolean hasNext() {
                return currentOrd < map.getValueCount();
              }

              @Override
              public BytesRef next() {
                if (!hasNext()) {
                  throw new NoSuchElementException();
                }
                int segmentNumber = map.getFirstSegmentNumber(currentOrd);
                long segmentOrd = map.getFirstSegmentOrd(currentOrd);
                final BytesRef term = dvs[segmentNumber].lookupOrd(segmentOrd);
                currentOrd++;
                return term;
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }
            };
          }
        },
        // doc -> ord count
        new Iterable<Number>() {
          @Override
          public Iterator<Number> iterator() {
            return new Iterator<Number>() {
              int readerUpto = -1;
              int docIDUpto;
              int nextValue;
              int currentMaxDoc;
              Bits currentLiveDocs;
              boolean nextIsSet;

              @Override
              public boolean hasNext() {
                return nextIsSet || setNext();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public Number next() {
                if (!hasNext()) {
                  throw new NoSuchElementException();
                }
                assert nextIsSet;
                nextIsSet = false;
                // TODO make a mutable number
                return nextValue;
              }

              private boolean setNext() {
                while (true) {
                  if (readerUpto == numReaders) {
                    return false;
                  }

                  if (docIDUpto == currentMaxDoc) {
                    readerUpto++;
                    if (readerUpto < numReaders) {
                      currentLiveDocs = mergeState.liveDocs[readerUpto];
                      currentMaxDoc = mergeState.maxDocs[readerUpto];
                    }
                    docIDUpto = 0;
                    continue;
                  }

                  if (currentLiveDocs == null || currentLiveDocs.get(docIDUpto)) {
                    nextIsSet = true;
                    SortedSetDocValues dv = dvs[readerUpto];
                    dv.setDocument(docIDUpto);
                    nextValue = 0;
                    while (dv.nextOrd() != SortedSetDocValues.NO_MORE_ORDS) {
                      nextValue++;
                    }
                    docIDUpto++;
                    return true;
                  }

                  docIDUpto++;
                }
              }
            };
          }
        },
        // ords
        new Iterable<Number>() {
          @Override
          public Iterator<Number> iterator() {
            return new Iterator<Number>() {
              int readerUpto = -1;
              int docIDUpto;
              long nextValue;
              int currentMaxDoc;
              Bits currentLiveDocs;
              LongValues currentMap;
              boolean nextIsSet;
              long ords[] = new long[8];
              int ordUpto;
              int ordLength;

              @Override
              public boolean hasNext() {
                return nextIsSet || setNext();
              }

              @Override
              public void remove() {
                throw new UnsupportedOperationException();
              }

              @Override
              public Number next() {
                if (!hasNext()) {
                  throw new NoSuchElementException();
                }
                assert nextIsSet;
                nextIsSet = false;
                // TODO make a mutable number
                return nextValue;
              }

              private boolean setNext() {
                while (true) {
                  if (readerUpto == numReaders) {
                    return false;
                  }
                  
                  if (ordUpto < ordLength) {
                    nextValue = ords[ordUpto];
                    ordUpto++;
                    nextIsSet = true;
                    return true;
                  }

                  if (docIDUpto == currentMaxDoc) {
                    readerUpto++;
                    if (readerUpto < numReaders) {
                      currentMap = map.getGlobalOrds(readerUpto);
                      currentLiveDocs = mergeState.liveDocs[readerUpto];
                      currentMaxDoc = mergeState.maxDocs[readerUpto];
                    }
                    docIDUpto = 0;
                    continue;
                  }
                  
                  if (currentLiveDocs == null || currentLiveDocs.get(docIDUpto)) {
                    assert docIDUpto < currentMaxDoc;
                    SortedSetDocValues dv = dvs[readerUpto];
                    dv.setDocument(docIDUpto);
                    ordUpto = ordLength = 0;
                    long ord;
                    while ((ord = dv.nextOrd()) != SortedSetDocValues.NO_MORE_ORDS) {
                      if (ordLength == ords.length) {
                        ords = ArrayUtil.grow(ords, ordLength+1);
                      }
                      ords[ordLength] = currentMap.get(ord);
                      ordLength++;
                    }
                    docIDUpto++;
                    continue;
                  }

                  docIDUpto++;
                }
              }
            };
          }
        }
     );
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
  public static Iterable<Number> singletonView(final Iterable<Number> docToValueCount, final Iterable<Number> values, final Number missingValue) {
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
