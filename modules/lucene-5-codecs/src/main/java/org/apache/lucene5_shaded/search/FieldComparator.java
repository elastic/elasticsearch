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

import org.apache.lucene5_shaded.index.BinaryDocValues;
import org.apache.lucene5_shaded.index.DocValues;
import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.index.NumericDocValues;
import org.apache.lucene5_shaded.index.SortedDocValues;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.BytesRefBuilder;

/**
 * Expert: a FieldComparator compares hits so as to determine their
 * sort order when collecting the top results with {@link
 * TopFieldCollector}.  The concrete public FieldComparator
 * classes here correspond to the SortField types.
 *
 * <p>This API is designed to achieve high performance
 * sorting, by exposing a tight interaction with {@link
 * FieldValueHitQueue} as it visits hits.  Whenever a hit is
 * competitive, it's enrolled into a virtual slot, which is
 * an int ranging from 0 to numHits-1. Segment transitions are
 * handled by creating a dedicated per-segment
 * {@link LeafFieldComparator} which also needs to interact
 * with the {@link FieldValueHitQueue} but can optimize based
 * on the segment to collect.</p>
 * 
 * <p>The following functions need to be implemented</p>
 * <ul>
 *  <li> {@link #compare} Compare a hit at 'slot a'
 *       with hit 'slot b'.
 * 
 *  <li> {@link #setTopValue} This method is called by
 *       {@link TopFieldCollector} to notify the
 *       FieldComparator of the top most value, which is
 *       used by future calls to
 *       {@link LeafFieldComparator#compareTop}.
 * 
 *  <li> {@link #getLeafComparator(LeafReaderContext)} Invoked
 *       when the search is switching to the next segment.
 *       You may need to update internal state of the
 *       comparator, for example retrieving new values from
 *       DocValues.
 *
 *  <li> {@link #value} Return the sort value stored in
 *       the specified slot.  This is only called at the end
 *       of the search, in order to populate {@link
 *       FieldDoc#fields} when returning the top results.
 * </ul>
 *
 * @see LeafFieldComparator
 * @lucene.experimental
 */
public abstract class FieldComparator<T> {

  /**
   * Compare hit at slot1 with hit at slot2.
   * 
   * @param slot1 first slot to compare
   * @param slot2 second slot to compare
   * @return any {@code N < 0} if slot2's value is sorted after
   * slot1, any {@code N > 0} if the slot2's value is sorted before
   * slot1 and {@code 0} if they are equal
   */
  public abstract int compare(int slot1, int slot2);

  /**
   * Record the top value, for future calls to {@link
   * LeafFieldComparator#compareTop}.  This is only called for searches that
   * use searchAfter (deep paging), and is called before any
   * calls to {@link #getLeafComparator(LeafReaderContext)}.
   */
  public abstract void setTopValue(T value);

  /**
   * Return the actual value in the slot.
   *
   * @param slot the value
   * @return value in this slot
   */
  public abstract T value(int slot);

  /**
   * Get a per-segment {@link LeafFieldComparator} to collect the given
   * {@link LeafReaderContext}. All docIDs supplied to
   * this {@link LeafFieldComparator} are relative to the current reader (you
   * must add docBase if you need to map it to a top-level docID).
   * 
   * @param context current reader context
   * @return the comparator to use for this segment
   * @throws IOException if there is a low-level IO error
   */
  public abstract LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException;

  /** Returns a negative integer if first is less than second,
   *  0 if they are equal and a positive integer otherwise. Default
   *  impl to assume the type implements Comparable and
   *  invoke .compareTo; be sure to override this method if
   *  your FieldComparator's type isn't a Comparable or
   *  if your values may sometimes be null */
  @SuppressWarnings("unchecked")
  public int compareValues(T first, T second) {
    if (first == null) {
      if (second == null) {
        return 0;
      } else {
        return -1;
      }
    } else if (second == null) {
      return 1;
    } else {
      return ((Comparable<T>) first).compareTo(second);
    }
  }


  /**
   * Base FieldComparator class for numeric types
   */
  public static abstract class NumericComparator<T extends Number> extends SimpleFieldComparator<T> {
    protected final T missingValue;
    protected final String field;
    protected Bits docsWithField;
    protected NumericDocValues currentReaderValues;
    
    public NumericComparator(String field, T missingValue) {
      this.field = field;
      this.missingValue = missingValue;
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      currentReaderValues = getNumericDocValues(context, field);
      if (missingValue != null) {
        docsWithField = getDocsWithValue(context, field);
        // optimization to remove unneeded checks on the bit interface:
        if (docsWithField instanceof Bits.MatchAllBits) {
          docsWithField = null;
        }
      } else {
        docsWithField = null;
      }
    }
    
    /** Retrieves the NumericDocValues for the field in this segment */
    protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
      return DocValues.getNumeric(context.reader(), field);
    }

    /** Retrieves a {@link Bits} instance representing documents that have a value in this segment. */
    protected Bits getDocsWithValue(LeafReaderContext context, String field) throws IOException {
      return DocValues.getDocsWithField(context.reader(), field);
    }
  }

  /** Parses field's values as double (using {@link
   *  org.apache.lucene5_shaded.index.LeafReader#getNumericDocValues} and sorts by ascending value */
  public static class DoubleComparator extends NumericComparator<Double> {
    private final double[] values;
    private double bottom;
    private double topValue;

    /** 
     * Creates a new comparator based on {@link Double#compare} for {@code numHits}.
     * When a document has no value for the field, {@code missingValue} is substituted. 
     */
    public DoubleComparator(int numHits, String field, Double missingValue) {
      super(field, missingValue);
      values = new double[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
      return Double.compare(values[slot1], values[slot2]);
    }

    @Override
    public int compareBottom(int doc) {
      double v2 = Double.longBitsToDouble(currentReaderValues.get(doc));
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

      return Double.compare(bottom, v2);
    }

    @Override
    public void copy(int slot, int doc) {
      double v2 = Double.longBitsToDouble(currentReaderValues.get(doc));
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

      values[slot] = v2;
    }
    
    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public void setTopValue(Double value) {
      topValue = value;
    }

    @Override
    public Double value(int slot) {
      return Double.valueOf(values[slot]);
    }

    @Override
    public int compareTop(int doc) {
      double docValue = Double.longBitsToDouble(currentReaderValues.get(doc));
      // Test for docValue == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && docValue == 0 && !docsWithField.get(doc)) {
        docValue = missingValue;
      }
      return Double.compare(topValue, docValue);
    }
  }

  /** Parses field's values as float (using {@link
   *  org.apache.lucene5_shaded.index.LeafReader#getNumericDocValues(String)} and sorts by ascending value */
  public static class FloatComparator extends NumericComparator<Float> {
    private final float[] values;
    private float bottom;
    private float topValue;

    /** 
     * Creates a new comparator based on {@link Float#compare} for {@code numHits}.
     * When a document has no value for the field, {@code missingValue} is substituted. 
     */
    public FloatComparator(int numHits, String field, Float missingValue) {
      super(field, missingValue);
      values = new float[numHits];
    }
    
    @Override
    public int compare(int slot1, int slot2) {
      return Float.compare(values[slot1], values[slot2]);
    }

    @Override
    public int compareBottom(int doc) {
      // TODO: are there sneaky non-branch ways to compute sign of float?
      float v2 = Float.intBitsToFloat((int)currentReaderValues.get(doc));
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

      return Float.compare(bottom, v2);
    }

    @Override
    public void copy(int slot, int doc) {
      float v2 =  Float.intBitsToFloat((int)currentReaderValues.get(doc));
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

      values[slot] = v2;
    }
    
    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public void setTopValue(Float value) {
      topValue = value;
    }

    @Override
    public Float value(int slot) {
      return Float.valueOf(values[slot]);
    }

    @Override
    public int compareTop(int doc) {
      float docValue = Float.intBitsToFloat((int)currentReaderValues.get(doc));
      // Test for docValue == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && docValue == 0 && !docsWithField.get(doc)) {
        docValue = missingValue;
      }
      return Float.compare(topValue, docValue);
    }
  }

  /** Parses field's values as int (using {@link
   *  org.apache.lucene5_shaded.index.LeafReader#getNumericDocValues(String)} and sorts by ascending value */
  public static class IntComparator extends NumericComparator<Integer> {
    private final int[] values;
    private int bottom;                           // Value of bottom of queue
    private int topValue;

    /** 
     * Creates a new comparator based on {@link Integer#compare} for {@code numHits}.
     * When a document has no value for the field, {@code missingValue} is substituted. 
     */
    public IntComparator(int numHits, String field, Integer missingValue) {
      super(field, missingValue);
      values = new int[numHits];
    }
        
    @Override
    public int compare(int slot1, int slot2) {
      return Integer.compare(values[slot1], values[slot2]);
    }

    @Override
    public int compareBottom(int doc) {
      int v2 = (int) currentReaderValues.get(doc);
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

      return Integer.compare(bottom, v2);
    }

    @Override
    public void copy(int slot, int doc) {
      int v2 = (int) currentReaderValues.get(doc);
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

      values[slot] = v2;
    }
    
    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public void setTopValue(Integer value) {
      topValue = value;
    }

    @Override
    public Integer value(int slot) {
      return Integer.valueOf(values[slot]);
    }

    @Override
    public int compareTop(int doc) {
      int docValue = (int) currentReaderValues.get(doc);
      // Test for docValue == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && docValue == 0 && !docsWithField.get(doc)) {
        docValue = missingValue;
      }
      return Integer.compare(topValue, docValue);
    }
  }

  /** Parses field's values as long (using {@link
   *  org.apache.lucene5_shaded.index.LeafReader#getNumericDocValues(String)} and sorts by ascending value */
  public static class LongComparator extends NumericComparator<Long> {
    private final long[] values;
    private long bottom;
    private long topValue;

    /** 
     * Creates a new comparator based on {@link Long#compare} for {@code numHits}.
     * When a document has no value for the field, {@code missingValue} is substituted. 
     */
    public LongComparator(int numHits, String field, Long missingValue) {
      super(field, missingValue);
      values = new long[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
      return Long.compare(values[slot1], values[slot2]);
    }

    @Override
    public int compareBottom(int doc) {
      // TODO: there are sneaky non-branch ways to compute
      // -1/+1/0 sign
      long v2 = currentReaderValues.get(doc);
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

      return Long.compare(bottom, v2);
    }

    @Override
    public void copy(int slot, int doc) {
      long v2 = currentReaderValues.get(doc);
      // Test for v2 == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && v2 == 0 && !docsWithField.get(doc)) {
        v2 = missingValue;
      }

      values[slot] = v2;
    }
    
    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public void setTopValue(Long value) {
      topValue = value;
    }

    @Override
    public Long value(int slot) {
      return Long.valueOf(values[slot]);
    }

    @Override
    public int compareTop(int doc) {
      long docValue = currentReaderValues.get(doc);
      // Test for docValue == 0 to save Bits.get method call for
      // the common case (doc has value and value is non-zero):
      if (docsWithField != null && docValue == 0 && !docsWithField.get(doc)) {
        docValue = missingValue;
      }
      return Long.compare(topValue, docValue);
    }
  }

  /** Sorts by descending relevance.  NOTE: if you are
   *  sorting only by descending relevance and then
   *  secondarily by ascending docID, performance is faster
   *  using {@link TopScoreDocCollector} directly (which {@link
   *  IndexSearcher#search} uses when no {@link Sort} is
   *  specified). */
  public static final class RelevanceComparator extends FieldComparator<Float> implements LeafFieldComparator {
    private final float[] scores;
    private float bottom;
    private Scorer scorer;
    private float topValue;

    /** Creates a new comparator based on relevance for {@code numHits}. */
    public RelevanceComparator(int numHits) {
      scores = new float[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
      return Float.compare(scores[slot2], scores[slot1]);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      float score = scorer.score();
      assert !Float.isNaN(score);
      return Float.compare(score, bottom);
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      scores[slot] = scorer.score();
      assert !Float.isNaN(scores[slot]);
    }

    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext context) {
      return this;
    }
    
    @Override
    public void setBottom(final int bottom) {
      this.bottom = scores[bottom];
    }

    @Override
    public void setTopValue(Float value) {
      topValue = value;
    }

    @Override
    public void setScorer(Scorer scorer) {
      // wrap with a ScoreCachingWrappingScorer so that successive calls to
      // score() will not incur score computation over and
      // over again.
      if (!(scorer instanceof ScoreCachingWrappingScorer)) {
        this.scorer = new ScoreCachingWrappingScorer(scorer);
      } else {
        this.scorer = scorer;
      }
    }
    
    @Override
    public Float value(int slot) {
      return Float.valueOf(scores[slot]);
    }

    // Override because we sort reverse of natural Float order:
    @Override
    public int compareValues(Float first, Float second) {
      // Reversed intentionally because relevance by default
      // sorts descending:
      return second.compareTo(first);
    }

    @Override
    public int compareTop(int doc) throws IOException {
      float docValue = scorer.score();
      assert !Float.isNaN(docValue);
      return Float.compare(docValue, topValue);
    }
  }

  /** Sorts by ascending docID */
  public static final class DocComparator extends FieldComparator<Integer> implements LeafFieldComparator {
    private final int[] docIDs;
    private int docBase;
    private int bottom;
    private int topValue;

    /** Creates a new comparator based on document ids for {@code numHits} */
    public DocComparator(int numHits) {
      docIDs = new int[numHits];
    }

    @Override
    public int compare(int slot1, int slot2) {
      // No overflow risk because docIDs are non-negative
      return docIDs[slot1] - docIDs[slot2];
    }

    @Override
    public int compareBottom(int doc) {
      // No overflow risk because docIDs are non-negative
      return bottom - (docBase + doc);
    }

    @Override
    public void copy(int slot, int doc) {
      docIDs[slot] = docBase + doc;
    }

    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext context) {
      // TODO: can we "map" our docIDs to the current
      // reader? saves having to then subtract on every
      // compare call
      this.docBase = context.docBase;
      return this;
    }
    
    @Override
    public void setBottom(final int bottom) {
      this.bottom = docIDs[bottom];
    }

    @Override
    public void setTopValue(Integer value) {
      topValue = value;
    }

    @Override
    public Integer value(int slot) {
      return Integer.valueOf(docIDs[slot]);
    }

    @Override
    public int compareTop(int doc) {
      int docValue = docBase + doc;
      return Integer.compare(topValue, docValue);
    }

    @Override
    public void setScorer(Scorer scorer) {}
  }
  
  /** Sorts by field's natural Term sort order, using
   *  ordinals.  This is functionally equivalent to {@link
   *  TermValComparator}, but it first resolves the string
   *  to their relative ordinal positions (using the index
   *  returned by {@link org.apache.lucene5_shaded.index.LeafReader#getSortedDocValues(String)}), and
   *  does most comparisons using the ordinals.  For medium
   *  to large results, this comparator will be much faster
   *  than {@link TermValComparator}.  For very small
   *  result sets it may be slower. */
  public static class TermOrdValComparator extends FieldComparator<BytesRef> implements LeafFieldComparator {
    /* Ords for each slot.
       @lucene5_shaded.internal */
    final int[] ords;

    /* Values for each slot.
       @lucene5_shaded.internal */
    final BytesRef[] values;
    private final BytesRefBuilder[] tempBRs;

    /* Which reader last copied a value into the slot. When
       we compare two slots, we just compare-by-ord if the
       readerGen is the same; else we must compare the
       values (slower).
       @lucene5_shaded.internal */
    final int[] readerGen;

    /* Gen of current reader we are on.
       @lucene5_shaded.internal */
    int currentReaderGen = -1;

    /* Current reader's doc ord/values.
       @lucene5_shaded.internal */
    SortedDocValues termsIndex;

    private final String field;

    /* Bottom slot, or -1 if queue isn't full yet
       @lucene5_shaded.internal */
    int bottomSlot = -1;

    /* Bottom ord (same as ords[bottomSlot] once bottomSlot
       is set).  Cached for faster compares.
       @lucene5_shaded.internal */
    int bottomOrd;

    /* True if current bottom slot matches the current
       reader.
       @lucene5_shaded.internal */
    boolean bottomSameReader;

    /* Bottom value (same as values[bottomSlot] once
       bottomSlot is set).  Cached for faster compares.
      @lucene5_shaded.internal */
    BytesRef bottomValue;

    /** Set by setTopValue. */
    BytesRef topValue;
    boolean topSameReader;
    int topOrd;

    /** -1 if missing values are sorted first, 1 if they are
     *  sorted last */
    final int missingSortCmp;
    
    /** Which ordinal to use for a missing value. */
    final int missingOrd;

    /** Creates this, sorting missing values first. */
    public TermOrdValComparator(int numHits, String field) {
      this(numHits, field, false);
    }

    /** Creates this, with control over how missing values
     *  are sorted.  Pass sortMissingLast=true to put
     *  missing values at the end. */
    public TermOrdValComparator(int numHits, String field, boolean sortMissingLast) {
      ords = new int[numHits];
      values = new BytesRef[numHits];
      tempBRs = new BytesRefBuilder[numHits];
      readerGen = new int[numHits];
      this.field = field;
      if (sortMissingLast) {
        missingSortCmp = 1;
        missingOrd = Integer.MAX_VALUE;
      } else {
        missingSortCmp = -1;
        missingOrd = -1;
      }
    }

    @Override
    public int compare(int slot1, int slot2) {
      if (readerGen[slot1] == readerGen[slot2]) {
        return ords[slot1] - ords[slot2];
      }

      final BytesRef val1 = values[slot1];
      final BytesRef val2 = values[slot2];
      if (val1 == null) {
        if (val2 == null) {
          return 0;
        }
        return missingSortCmp;
      } else if (val2 == null) {
        return -missingSortCmp;
      }
      return val1.compareTo(val2);
    }

    @Override
    public int compareBottom(int doc) {
      assert bottomSlot != -1;
      int docOrd = termsIndex.getOrd(doc);
      if (docOrd == -1) {
        docOrd = missingOrd;
      }
      if (bottomSameReader) {
        // ord is precisely comparable, even in the equal case
        return bottomOrd - docOrd;
      } else if (bottomOrd >= docOrd) {
        // the equals case always means bottom is > doc
        // (because we set bottomOrd to the lower bound in
        // setBottom):
        return 1;
      } else {
        return -1;
      }
    }

    @Override
    public void copy(int slot, int doc) {
      int ord = termsIndex.getOrd(doc);
      if (ord == -1) {
        ord = missingOrd;
        values[slot] = null;
      } else {
        assert ord >= 0;
        if (tempBRs[slot] == null) {
          tempBRs[slot] = new BytesRefBuilder();
        }
        tempBRs[slot].copyBytes(termsIndex.lookupOrd(ord));
        values[slot] = tempBRs[slot].get();
      }
      ords[slot] = ord;
      readerGen[slot] = currentReaderGen;
    }
    
    /** Retrieves the SortedDocValues for the field in this segment */
    protected SortedDocValues getSortedDocValues(LeafReaderContext context, String field) throws IOException {
      return DocValues.getSorted(context.reader(), field);
    }
    
    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
      termsIndex = getSortedDocValues(context, field);
      currentReaderGen++;

      if (topValue != null) {
        // Recompute topOrd/SameReader
        int ord = termsIndex.lookupTerm(topValue);
        if (ord >= 0) {
          topSameReader = true;
          topOrd = ord;
        } else {
          topSameReader = false;
          topOrd = -ord-2;
        }
      } else {
        topOrd = missingOrd;
        topSameReader = true;
      }
      //System.out.println("  getLeafComparator topOrd=" + topOrd + " topSameReader=" + topSameReader);

      if (bottomSlot != -1) {
        // Recompute bottomOrd/SameReader
        setBottom(bottomSlot);
      }

      return this;
    }
    
    @Override
    public void setBottom(final int bottom) {
      bottomSlot = bottom;

      bottomValue = values[bottomSlot];
      if (currentReaderGen == readerGen[bottomSlot]) {
        bottomOrd = ords[bottomSlot];
        bottomSameReader = true;
      } else {
        if (bottomValue == null) {
          // missingOrd is null for all segments
          assert ords[bottomSlot] == missingOrd;
          bottomOrd = missingOrd;
          bottomSameReader = true;
          readerGen[bottomSlot] = currentReaderGen;
        } else {
          final int ord = termsIndex.lookupTerm(bottomValue);
          if (ord < 0) {
            bottomOrd = -ord - 2;
            bottomSameReader = false;
          } else {
            bottomOrd = ord;
            // exact value match
            bottomSameReader = true;
            readerGen[bottomSlot] = currentReaderGen;            
            ords[bottomSlot] = bottomOrd;
          }
        }
      }
    }

    @Override
    public void setTopValue(BytesRef value) {
      // null is fine: it means the last doc of the prior
      // search was missing this value
      topValue = value;
      //System.out.println("setTopValue " + topValue);
    }

    @Override
    public BytesRef value(int slot) {
      return values[slot];
    }

    @Override
    public int compareTop(int doc) {

      int ord = termsIndex.getOrd(doc);
      if (ord == -1) {
        ord = missingOrd;
      }

      if (topSameReader) {
        // ord is precisely comparable, even in the equal
        // case
        //System.out.println("compareTop doc=" + doc + " ord=" + ord + " ret=" + (topOrd-ord));
        return topOrd - ord;
      } else if (ord <= topOrd) {
        // the equals case always means doc is < value
        // (because we set lastOrd to the lower bound)
        return 1;
      } else {
        return -1;
      }
    }

    @Override
    public int compareValues(BytesRef val1, BytesRef val2) {
      if (val1 == null) {
        if (val2 == null) {
          return 0;
        }
        return missingSortCmp;
      } else if (val2 == null) {
        return -missingSortCmp;
      }
      return val1.compareTo(val2);
    }

    @Override
    public void setScorer(Scorer scorer) {}
  }
  
  /** Sorts by field's natural Term sort order.  All
   *  comparisons are done using BytesRef.compareTo, which is
   *  slow for medium to large result sets but possibly
   *  very fast for very small results sets. */
  public static class TermValComparator extends FieldComparator<BytesRef> implements LeafFieldComparator {
    
    private final BytesRef[] values;
    private final BytesRefBuilder[] tempBRs;
    private BinaryDocValues docTerms;
    private Bits docsWithField;
    private final String field;
    private BytesRef bottom;
    private BytesRef topValue;
    private final int missingSortCmp;

    /** Sole constructor. */
    public TermValComparator(int numHits, String field, boolean sortMissingLast) {
      values = new BytesRef[numHits];
      tempBRs = new BytesRefBuilder[numHits];
      this.field = field;
      missingSortCmp = sortMissingLast ? 1 : -1;
    }

    @Override
    public int compare(int slot1, int slot2) {
      final BytesRef val1 = values[slot1];
      final BytesRef val2 = values[slot2];
      return compareValues(val1, val2);
    }

    @Override
    public int compareBottom(int doc) {
      final BytesRef comparableBytes = getComparableBytes(doc, docTerms.get(doc));
      return compareValues(bottom, comparableBytes);
    }

    @Override
    public void copy(int slot, int doc) {
      final BytesRef comparableBytes = getComparableBytes(doc, docTerms.get(doc));
      if (comparableBytes == null) {
        values[slot] = null;
      } else {
        if (tempBRs[slot] == null) {
          tempBRs[slot] = new BytesRefBuilder();
        }
        tempBRs[slot].copyBytes(comparableBytes);
        values[slot] = tempBRs[slot].get();
      }
    }

    /** Retrieves the BinaryDocValues for the field in this segment */
    protected BinaryDocValues getBinaryDocValues(LeafReaderContext context, String field) throws IOException {
      return DocValues.getBinary(context.reader(), field);
    }

    /** Retrieves the set of documents that have a value in this segment */
    protected Bits getDocsWithField(LeafReaderContext context, String field) throws IOException {
      return DocValues.getDocsWithField(context.reader(), field);
    }

    /** Check whether the given value represents <tt>null</tt>. This can be
     *  useful if the {@link BinaryDocValues} returned by {@link #getBinaryDocValues}
     *  use a special value as a sentinel. The default implementation checks
     *  {@link #getDocsWithField}.
     *  <p>NOTE: The null value can only be an EMPTY {@link BytesRef}. */
    protected boolean isNull(int doc, BytesRef term) {
      return docsWithField != null && docsWithField.get(doc) == false;
    }

    @Override
    public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
      docTerms = getBinaryDocValues(context, field);
      docsWithField = getDocsWithField(context, field);
      if (docsWithField instanceof Bits.MatchAllBits) {
        docsWithField = null;
      }
      return this;
    }
    
    @Override
    public void setBottom(final int bottom) {
      this.bottom = values[bottom];
    }

    @Override
    public void setTopValue(BytesRef value) {
      // null is fine: it means the last doc of the prior
      // search was missing this value
      topValue = value;
    }

    @Override
    public BytesRef value(int slot) {
      return values[slot];
    }

    @Override
    public int compareValues(BytesRef val1, BytesRef val2) {
      // missing always sorts first:
      if (val1 == null) {
        if (val2 == null) {
          return 0;
        }
        return missingSortCmp;
      } else if (val2 == null) {
        return -missingSortCmp;
      }
      return val1.compareTo(val2);
    }

    @Override
    public int compareTop(int doc) {
      final BytesRef comparableBytes = getComparableBytes(doc, docTerms.get(doc));
      return compareValues(topValue, comparableBytes);
    }

    /**
     * Given a document and a term, return the term itself if it exists or
     * <tt>null</tt> otherwise.
     */
    private BytesRef getComparableBytes(int doc, BytesRef term) {
      if (term.length == 0 && isNull(doc, term)) {
        return null;
      }
      return term;
    }

    @Override
    public void setScorer(Scorer scorer) {}
  }
}
