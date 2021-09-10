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
import java.util.Arrays;

import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.BytesRef;

/** 
 * This class contains utility methods and constants for DocValues
 */
public final class DocValues {

  /* no instantiation */
  private DocValues() {}

  /** 
   * An empty BinaryDocValues which returns {@link BytesRef#EMPTY_BYTES} for every document 
   */
  public static final BinaryDocValues emptyBinary() {
    final BytesRef empty = new BytesRef();
    return new BinaryDocValues() {
      @Override
      public BytesRef get(int docID) {
        return empty;
      }
    };
  }

  /** 
   * An empty NumericDocValues which returns zero for every document 
   */
  public static final NumericDocValues emptyNumeric() {
    return new NumericDocValues() {
      @Override
      public long get(int docID) {
        return 0;
      }
    };
  }

  /** 
   * An empty SortedDocValues which returns {@link BytesRef#EMPTY_BYTES} for every document 
   */
  public static final SortedDocValues emptySorted() {
    final BytesRef empty = new BytesRef();
    return new SortedDocValues() {
      @Override
      public int getOrd(int docID) {
        return -1;
      }

      @Override
      public BytesRef lookupOrd(int ord) {
        return empty;
      }

      @Override
      public int getValueCount() {
        return 0;
      }
    };
  }

  /**
   * An empty SortedNumericDocValues which returns zero values for every document 
   */
  public static final SortedNumericDocValues emptySortedNumeric(int maxDoc) {
    return singleton(emptyNumeric(), new Bits.MatchNoBits(maxDoc));
  }

  /** 
   * An empty SortedDocValues which returns {@link SortedSetDocValues#NO_MORE_ORDS} for every document 
   */
  public static final RandomAccessOrds emptySortedSet() {
    return singleton(emptySorted());
  }
  
  /** 
   * Returns a multi-valued view over the provided SortedDocValues 
   */
  public static RandomAccessOrds singleton(SortedDocValues dv) {
    return new SingletonSortedSetDocValues(dv);
  }
  
  /** 
   * Returns a single-valued view of the SortedSetDocValues, if it was previously
   * wrapped with {@link #singleton(SortedDocValues)}, or null. 
   */
  public static SortedDocValues unwrapSingleton(SortedSetDocValues dv) {
    if (dv instanceof SingletonSortedSetDocValues) {
      return ((SingletonSortedSetDocValues)dv).getSortedDocValues();
    } else {
      return null;
    }
  }
  
  /** 
   * Returns a single-valued view of the SortedNumericDocValues, if it was previously
   * wrapped with {@link #singleton(NumericDocValues, Bits)}, or null. 
   * @see #unwrapSingletonBits(SortedNumericDocValues)
   */
  public static NumericDocValues unwrapSingleton(SortedNumericDocValues dv) {
    if (dv instanceof SingletonSortedNumericDocValues) {
      return ((SingletonSortedNumericDocValues)dv).getNumericDocValues();
    } else {
      return null;
    }
  }
  
  /** 
   * Returns the documents with a value for the SortedNumericDocValues, if it was previously
   * wrapped with {@link #singleton(NumericDocValues, Bits)}, or null. 
   */
  public static Bits unwrapSingletonBits(SortedNumericDocValues dv) {
    if (dv instanceof SingletonSortedNumericDocValues) {
      return ((SingletonSortedNumericDocValues)dv).getDocsWithField();
    } else {
      return null;
    }
  }
  
  /**
   * Returns a multi-valued view over the provided NumericDocValues
   */
  public static SortedNumericDocValues singleton(NumericDocValues dv, Bits docsWithField) {
    return new SingletonSortedNumericDocValues(dv, docsWithField);
  }
  
  /**
   * Returns a Bits representing all documents from <code>dv</code> that have a value.
   */
  public static Bits docsWithValue(final SortedDocValues dv, final int maxDoc) {
    return new Bits() {
      @Override
      public boolean get(int index) {
        return dv.getOrd(index) >= 0;
      }

      @Override
      public int length() {
        return maxDoc;
      }
    };
  }
  
  /**
   * Returns a Bits representing all documents from <code>dv</code> that have a value.
   */
  public static Bits docsWithValue(final SortedSetDocValues dv, final int maxDoc) {
    return new Bits() {
      @Override
      public boolean get(int index) {
        dv.setDocument(index);
        return dv.nextOrd() != SortedSetDocValues.NO_MORE_ORDS;
      }

      @Override
      public int length() {
        return maxDoc;
      }
    };
  }
  
  /**
   * Returns a Bits representing all documents from <code>dv</code> that have a value.
   */
  public static Bits docsWithValue(final SortedNumericDocValues dv, final int maxDoc) {
    return new Bits() {
      @Override
      public boolean get(int index) {
        dv.setDocument(index);
        return dv.count() != 0;
      }

      @Override
      public int length() {
        return maxDoc;
      }
    };
  }
  
  // some helpers, for transition from fieldcache apis.
  // as opposed to the LeafReader apis (which must be strict for consistency), these are lenient
  
  // helper method: to give a nice error when LeafReader.getXXXDocValues returns null.
  private static void checkField(LeafReader in, String field, DocValuesType... expected) {
    FieldInfo fi = in.getFieldInfos().fieldInfo(field);
    if (fi != null) {
      DocValuesType actual = fi.getDocValuesType();
      throw new IllegalStateException("unexpected docvalues type " + actual + 
                                        " for field '" + field + "' " +
                                        (expected.length == 1 
                                        ? "(expected=" + expected[0]
                                        : "(expected one of " + Arrays.toString(expected)) + "). " +
                                        "Use UninvertingReader or index with docvalues.");
    }
  }
  
  /**
   * Returns NumericDocValues for the field, or {@link #emptyNumeric()} if it has none. 
   * @return docvalues instance, or an empty instance if {@code field} does not exist in this reader.
   * @throws IllegalStateException if {@code field} exists, but was not indexed with docvalues.
   * @throws IllegalStateException if {@code field} has docvalues, but the type is not {@link DocValuesType#NUMERIC}.
   * @throws IOException if an I/O error occurs.
   */
  public static NumericDocValues getNumeric(LeafReader reader, String field) throws IOException {
    NumericDocValues dv = reader.getNumericDocValues(field);
    if (dv == null) {
      checkField(reader, field, DocValuesType.NUMERIC);
      return emptyNumeric();
    } else {
      return dv;
    }
  }
  
  /**
   * Returns BinaryDocValues for the field, or {@link #emptyBinary} if it has none. 
   * @return docvalues instance, or an empty instance if {@code field} does not exist in this reader.
   * @throws IllegalStateException if {@code field} exists, but was not indexed with docvalues.
   * @throws IllegalStateException if {@code field} has docvalues, but the type is not {@link DocValuesType#BINARY}
   *                               or {@link DocValuesType#SORTED}.
   * @throws IOException if an I/O error occurs.
   */
  public static BinaryDocValues getBinary(LeafReader reader, String field) throws IOException {
    BinaryDocValues dv = reader.getBinaryDocValues(field);
    if (dv == null) {
      dv = reader.getSortedDocValues(field);
      if (dv == null) {
        checkField(reader, field, DocValuesType.BINARY, DocValuesType.SORTED);
        return emptyBinary();
      }
    }
    return dv;
  }
  
  /**
   * Returns SortedDocValues for the field, or {@link #emptySorted} if it has none. 
   * @return docvalues instance, or an empty instance if {@code field} does not exist in this reader.
   * @throws IllegalStateException if {@code field} exists, but was not indexed with docvalues.
   * @throws IllegalStateException if {@code field} has docvalues, but the type is not {@link DocValuesType#SORTED}.
   * @throws IOException if an I/O error occurs.
   */
  public static SortedDocValues getSorted(LeafReader reader, String field) throws IOException {
    SortedDocValues dv = reader.getSortedDocValues(field);
    if (dv == null) {
      checkField(reader, field, DocValuesType.SORTED);
      return emptySorted();
    } else {
      return dv;
    }
  }
  
  /**
   * Returns SortedNumericDocValues for the field, or {@link #emptySortedNumeric} if it has none. 
   * @return docvalues instance, or an empty instance if {@code field} does not exist in this reader.
   * @throws IllegalStateException if {@code field} exists, but was not indexed with docvalues.
   * @throws IllegalStateException if {@code field} has docvalues, but the type is not {@link DocValuesType#SORTED_NUMERIC}
   *                               or {@link DocValuesType#NUMERIC}.
   * @throws IOException if an I/O error occurs.
   */
  public static SortedNumericDocValues getSortedNumeric(LeafReader reader, String field) throws IOException {
    SortedNumericDocValues dv = reader.getSortedNumericDocValues(field);
    if (dv == null) {
      NumericDocValues single = reader.getNumericDocValues(field);
      if (single == null) {
        checkField(reader, field, DocValuesType.SORTED_NUMERIC, DocValuesType.NUMERIC);
        return emptySortedNumeric(reader.maxDoc());
      }
      Bits bits = reader.getDocsWithField(field);
      return singleton(single, bits);
    }
    return dv;
  }
  
  /**
   * Returns SortedSetDocValues for the field, or {@link #emptySortedSet} if it has none. 
   * @return docvalues instance, or an empty instance if {@code field} does not exist in this reader.
   * @throws IllegalStateException if {@code field} exists, but was not indexed with docvalues.
   * @throws IllegalStateException if {@code field} has docvalues, but the type is not {@link DocValuesType#SORTED_SET}
   *                               or {@link DocValuesType#SORTED}.
   * @throws IOException if an I/O error occurs.
   */
  public static SortedSetDocValues getSortedSet(LeafReader reader, String field) throws IOException {
    SortedSetDocValues dv = reader.getSortedSetDocValues(field);
    if (dv == null) {
      SortedDocValues sorted = reader.getSortedDocValues(field);
      if (sorted == null) {
        checkField(reader, field, DocValuesType.SORTED, DocValuesType.SORTED_SET);
        return emptySortedSet();
      }
      return singleton(sorted);
    }
    return dv;
  }
  
  /**
   * Returns Bits for the field, or {@link Bits} matching nothing if it has none. 
   * @return bits instance, or an empty instance if {@code field} does not exist in this reader.
   * @throws IllegalStateException if {@code field} exists, but was not indexed with docvalues.
   * @throws IOException if an I/O error occurs.
   */
  public static Bits getDocsWithField(LeafReader reader, String field) throws IOException {
    Bits dv = reader.getDocsWithField(field);
    if (dv == null) {
      assert DocValuesType.values().length == 6; // we just don't want NONE
      checkField(reader, field, DocValuesType.BINARY, 
                            DocValuesType.NUMERIC, 
                            DocValuesType.SORTED, 
                            DocValuesType.SORTED_NUMERIC, 
                            DocValuesType.SORTED_SET);
      return new Bits.MatchNoBits(reader.maxDoc());
    } else {
      return dv;
    }
  }
}
