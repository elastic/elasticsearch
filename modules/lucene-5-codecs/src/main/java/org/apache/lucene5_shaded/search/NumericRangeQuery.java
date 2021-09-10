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
import java.util.LinkedList;
import java.util.Objects;

import org.apache.lucene5_shaded.analysis.NumericTokenStream; // for javadocs
import org.apache.lucene5_shaded.document.DoubleField; // for javadocs
import org.apache.lucene5_shaded.document.FloatField; // for javadocs
import org.apache.lucene5_shaded.document.IntField; // for javadocs
import org.apache.lucene5_shaded.document.LongField; // for javadocs
import org.apache.lucene5_shaded.document.FieldType.NumericType;
import org.apache.lucene5_shaded.index.FilteredTermsEnum;
import org.apache.lucene5_shaded.index.Terms;
import org.apache.lucene5_shaded.index.TermsEnum;
import org.apache.lucene5_shaded.util.AttributeSource;
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.util.NumericUtils;
import org.apache.lucene5_shaded.util.ToStringUtils;
import org.apache.lucene5_shaded.index.Term; // for javadocs

/**
 * <p>A {@link Query} that matches numeric values within a
 * specified range.  To use this, you must first index the
 * numeric values using {@link IntField}, {@link
 * FloatField}, {@link LongField} or {@link DoubleField} (expert: {@link
 * NumericTokenStream}).  If your terms are instead textual,
 * you should use {@link TermRangeQuery}.</p>
 *
 * <p>You create a new NumericRangeQuery with the static
 * factory methods, eg:
 *
 * <pre class="prettyprint">
 * Query q = NumericRangeQuery.newFloatRange("weight", 0.03f, 0.10f, true, true);
 * </pre>
 *
 * matches all documents whose float valued "weight" field
 * ranges from 0.03 to 0.10, inclusive.
 *
 * <p>The performance of NumericRangeQuery is much better
 * than the corresponding {@link TermRangeQuery} because the
 * number of terms that must be searched is usually far
 * fewer, thanks to trie indexing, described below.</p>
 *
 * <p>You can optionally specify a <a
 * href="#precisionStepDesc"><code>precisionStep</code></a>
 * when creating this query.  This is necessary if you've
 * changed this configuration from its default (4) during
 * indexing.  Lower values consume more disk space but speed
 * up searching.  Suitable values are between <b>1</b> and
 * <b>8</b>. A good starting point to test is <b>4</b>,
 * which is the default value for all <code>Numeric*</code>
 * classes.  See <a href="#precisionStepDesc">below</a> for
 * details.
 *
 * <p>This query defaults to {@linkplain
 * MultiTermQuery#CONSTANT_SCORE_REWRITE}.
 * With precision steps of &le;4, this query can be run with
 * one of the BooleanQuery rewrite methods without changing
 * BooleanQuery's default max clause count.
 *
 * <br><h3>How it works</h3>
 *
 * <p>See the publication about <a target="_blank" href="http://www.panfmp.org">panFMP</a>,
 * where this algorithm was described (referred to as <code>TrieRangeQuery</code>):
 *
 * <blockquote><strong>Schindler, U, Diepenbroek, M</strong>, 2008.
 * <em>Generic XML-based Framework for Metadata Portals.</em>
 * Computers &amp; Geosciences 34 (12), 1947-1955.
 * <a href="http://dx.doi.org/10.1016/j.cageo.2008.02.023"
 * target="_blank">doi:10.1016/j.cageo.2008.02.023</a></blockquote>
 *
 * <p><em>A quote from this paper:</em> Because Apache Lucene is a full-text
 * search engine and not a conventional database, it cannot handle numerical ranges
 * (e.g., field value is inside user defined bounds, even dates are numerical values).
 * We have developed an extension to Apache Lucene that stores
 * the numerical values in a special string-encoded format with variable precision
 * (all numerical values like doubles, longs, floats, and ints are converted to
 * lexicographic sortable string representations and stored with different precisions
 * (for a more detailed description of how the values are stored,
 * see {@link NumericUtils}). A range is then divided recursively into multiple intervals for searching:
 * The center of the range is searched only with the lowest possible precision in the <em>trie</em>,
 * while the boundaries are matched more exactly. This reduces the number of terms dramatically.</p>
 *
 * <p>For the variant that stores long values in 8 different precisions (each reduced by 8 bits) that
 * uses a lowest precision of 1 byte, the index contains only a maximum of 256 distinct values in the
 * lowest precision. Overall, a range could consist of a theoretical maximum of
 * <code>7*255*2 + 255 = 3825</code> distinct terms (when there is a term for every distinct value of an
 * 8-byte-number in the index and the range covers almost all of them; a maximum of 255 distinct values is used
 * because it would always be possible to reduce the full 256 values to one term with degraded precision).
 * In practice, we have seen up to 300 terms in most cases (index with 500,000 metadata records
 * and a uniform value distribution).</p>
 *
 * <h3><a name="precisionStepDesc">Precision Step</a></h3>
 * <p>You can choose any <code>precisionStep</code> when encoding values.
 * Lower step values mean more precisions and so more terms in index (and index gets larger). The number
 * of indexed terms per value is (those are generated by {@link NumericTokenStream}):
 * <p style="font-family:serif">
 * &nbsp;&nbsp;indexedTermsPerValue = <b>ceil</b><big>(</big>bitsPerValue / precisionStep<big>)</big>
 * </p>
 * As the lower precision terms are shared by many values, the additional terms only
 * slightly grow the term dictionary (approx. 7% for <code>precisionStep=4</code>), but have a larger
 * impact on the postings (the postings file will have  more entries, as every document is linked to
 * <code>indexedTermsPerValue</code> terms instead of one). The formula to estimate the growth
 * of the term dictionary in comparison to one term per value:
 * <p>
 * <!-- the formula in the alt attribute was transformed from latex to PNG with http://1.618034.com/latex.php (with 110 dpi): -->
 * &nbsp;&nbsp;<img src="doc-files/nrq-formula-1.png" alt="\mathrm{termDictOverhead} = \sum\limits_{i=0}^{\mathrm{indexedTermsPerValue}-1} \frac{1}{2^{\mathrm{precisionStep}\cdot i}}">
 * </p>
 * <p>On the other hand, if the <code>precisionStep</code> is smaller, the maximum number of terms to match reduces,
 * which optimizes query speed. The formula to calculate the maximum number of terms that will be visited while
 * executing the query is:
 * <p>
 * <!-- the formula in the alt attribute was transformed from latex to PNG with http://1.618034.com/latex.php (with 110 dpi): -->
 * &nbsp;&nbsp;<img src="doc-files/nrq-formula-2.png" alt="\mathrm{maxQueryTerms} = \left[ \left( \mathrm{indexedTermsPerValue} - 1 \right) \cdot \left(2^\mathrm{precisionStep} - 1 \right) \cdot 2 \right] + \left( 2^\mathrm{precisionStep} - 1 \right)">
 * </p>
 * <p>For longs stored using a precision step of 4, <code>maxQueryTerms = 15*15*2 + 15 = 465</code>, and for a precision
 * step of 2, <code>maxQueryTerms = 31*3*2 + 3 = 189</code>. But the faster search speed is reduced by more seeking
 * in the term enum of the index. Because of this, the ideal <code>precisionStep</code> value can only
 * be found out by testing. <b>Important:</b> You can index with a lower precision step value and test search speed
 * using a multiple of the original step value.</p>
 *
 * <p>Good values for <code>precisionStep</code> are depending on usage and data type:
 * <ul>
 *  <li>The default for all data types is <b>4</b>, which is used, when no <code>precisionStep</code> is given.
 *  <li>Ideal value in most cases for <em>64 bit</em> data types <em>(long, double)</em> is <b>6</b> or <b>8</b>.
 *  <li>Ideal value in most cases for <em>32 bit</em> data types <em>(int, float)</em> is <b>4</b>.
 *  <li>For low cardinality fields larger precision steps are good. If the cardinality is &lt; 100, it is
 *  fair to use {@link Integer#MAX_VALUE} (see below).
 *  <li>Steps <b>&ge;64</b> for <em>long/double</em> and <b>&ge;32</b> for <em>int/float</em> produces one token
 *  per value in the index and querying is as slow as a conventional {@link TermRangeQuery}. But it can be used
 *  to produce fields, that are solely used for sorting (in this case simply use {@link Integer#MAX_VALUE} as
 *  <code>precisionStep</code>). Using {@link IntField},
 *  {@link LongField}, {@link FloatField} or {@link DoubleField} for sorting
 *  is ideal, because building the field cache is much faster than with text-only numbers.
 *  These fields have one term per value and therefore also work with term enumeration for building distinct lists
 *  (e.g. facets / preselected values to search for).
 *  Sorting is also possible with range query optimized fields using one of the above <code>precisionSteps</code>.
 * </ul>
 *
 * <p>Comparisons of the different types of RangeQueries on an index with about 500,000 docs showed
 * that {@link TermRangeQuery} in boolean rewrite mode (with raised {@link BooleanQuery} clause count)
 * took about 30-40 secs to complete, {@link TermRangeQuery} in constant score filter rewrite mode took 5 secs
 * and executing this class took &lt;100ms to complete (on an Opteron64 machine, Java 1.5, 8 bit
 * precision step). This query type was developed for a geographic portal, where the performance for
 * e.g. bounding boxes or exact date/time stamps is important.</p>
 *
 * @since 2.9
 **/
public final class NumericRangeQuery<T extends Number> extends MultiTermQuery {

  private NumericRangeQuery(final String field, final int precisionStep, final NumericType dataType,
    T min, T max, final boolean minInclusive, final boolean maxInclusive) {
    super(field);
    if (precisionStep < 1)
      throw new IllegalArgumentException("precisionStep must be >=1");
    this.precisionStep = precisionStep;
    this.dataType = Objects.requireNonNull(dataType, "NumericType must not be null");
    this.min = min;
    this.max = max;
    this.minInclusive = minInclusive;
    this.maxInclusive = maxInclusive;
  }
  
  /**
   * Factory that creates a <code>NumericRangeQuery</code>, that queries a <code>long</code>
   * range using the given <a href="#precisionStepDesc"><code>precisionStep</code></a>.
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeQuery<Long> newLongRange(final String field, final int precisionStep,
    Long min, Long max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeQuery<>(field, precisionStep, NumericType.LONG, min, max, minInclusive, maxInclusive);
  }
  
  /**
   * Factory that creates a <code>NumericRangeQuery</code>, that queries a <code>long</code>
   * range using the default <code>precisionStep</code> {@link NumericUtils#PRECISION_STEP_DEFAULT} (16).
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeQuery<Long> newLongRange(final String field,
    Long min, Long max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeQuery<>(field, NumericUtils.PRECISION_STEP_DEFAULT, NumericType.LONG, min, max, minInclusive, maxInclusive);
  }
  
  /**
   * Factory that creates a <code>NumericRangeQuery</code>, that queries a <code>int</code>
   * range using the given <a href="#precisionStepDesc"><code>precisionStep</code></a>.
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeQuery<Integer> newIntRange(final String field, final int precisionStep,
    Integer min, Integer max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeQuery<>(field, precisionStep, NumericType.INT, min, max, minInclusive, maxInclusive);
  }
  
  /**
   * Factory that creates a <code>NumericRangeQuery</code>, that queries a <code>int</code>
   * range using the default <code>precisionStep</code> {@link NumericUtils#PRECISION_STEP_DEFAULT_32} (8).
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeQuery<Integer> newIntRange(final String field,
    Integer min, Integer max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeQuery<>(field, NumericUtils.PRECISION_STEP_DEFAULT_32, NumericType.INT, min, max, minInclusive, maxInclusive);
  }
  
  /**
   * Factory that creates a <code>NumericRangeQuery</code>, that queries a <code>double</code>
   * range using the given <a href="#precisionStepDesc"><code>precisionStep</code></a>.
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>.
   * {@link Double#NaN} will never match a half-open range, to hit {@code NaN} use a query
   * with {@code min == max == Double.NaN}.  By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeQuery<Double> newDoubleRange(final String field, final int precisionStep,
    Double min, Double max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeQuery<>(field, precisionStep, NumericType.DOUBLE, min, max, minInclusive, maxInclusive);
  }
  
  /**
   * Factory that creates a <code>NumericRangeQuery</code>, that queries a <code>double</code>
   * range using the default <code>precisionStep</code> {@link NumericUtils#PRECISION_STEP_DEFAULT} (16).
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>.
   * {@link Double#NaN} will never match a half-open range, to hit {@code NaN} use a query
   * with {@code min == max == Double.NaN}.  By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeQuery<Double> newDoubleRange(final String field,
    Double min, Double max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeQuery<>(field, NumericUtils.PRECISION_STEP_DEFAULT, NumericType.DOUBLE, min, max, minInclusive, maxInclusive);
  }
  
  /**
   * Factory that creates a <code>NumericRangeQuery</code>, that queries a <code>float</code>
   * range using the given <a href="#precisionStepDesc"><code>precisionStep</code></a>.
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>.
   * {@link Float#NaN} will never match a half-open range, to hit {@code NaN} use a query
   * with {@code min == max == Float.NaN}.  By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeQuery<Float> newFloatRange(final String field, final int precisionStep,
    Float min, Float max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeQuery<>(field, precisionStep, NumericType.FLOAT, min, max, minInclusive, maxInclusive);
  }
  
  /**
   * Factory that creates a <code>NumericRangeQuery</code>, that queries a <code>float</code>
   * range using the default <code>precisionStep</code> {@link NumericUtils#PRECISION_STEP_DEFAULT_32} (8).
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>.
   * {@link Float#NaN} will never match a half-open range, to hit {@code NaN} use a query
   * with {@code min == max == Float.NaN}.  By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeQuery<Float> newFloatRange(final String field,
    Float min, Float max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeQuery<>(field, NumericUtils.PRECISION_STEP_DEFAULT_32, NumericType.FLOAT, min, max, minInclusive, maxInclusive);
  }

  @Override @SuppressWarnings("unchecked")
  protected TermsEnum getTermsEnum(final Terms terms, AttributeSource atts) throws IOException {
    // very strange: java.lang.Number itself is not Comparable, but all subclasses used here are
    if (min != null && max != null && ((Comparable<T>) min).compareTo(max) > 0) {
      return TermsEnum.EMPTY;
    }
    return new NumericRangeTermsEnum(terms.iterator());
  }

  /** Returns <code>true</code> if the lower endpoint is inclusive */
  public boolean includesMin() { return minInclusive; }
  
  /** Returns <code>true</code> if the upper endpoint is inclusive */
  public boolean includesMax() { return maxInclusive; }

  /** Returns the lower value of this range query */
  public T getMin() { return min; }

  /** Returns the upper value of this range query */
  public T getMax() { return max; }
  
  /** Returns the precision step. */
  public int getPrecisionStep() { return precisionStep; }
  
  @Override
  public String toString(final String field) {
    final StringBuilder sb = new StringBuilder();
    if (!getField().equals(field)) sb.append(getField()).append(':');
    return sb.append(minInclusive ? '[' : '{')
      .append((min == null) ? "*" : min.toString())
      .append(" TO ")
      .append((max == null) ? "*" : max.toString())
      .append(maxInclusive ? ']' : '}')
      .append(ToStringUtils.boost(getBoost()))
      .toString();
  }

  @Override
  @SuppressWarnings({"unchecked","rawtypes"})
  public final boolean equals(final Object o) {
    if (o==this) return true;
    if (!super.equals(o))
      return false;
    if (o instanceof NumericRangeQuery) {
      final NumericRangeQuery q=(NumericRangeQuery)o;
      return (
        (q.min == null ? min == null : q.min.equals(min)) &&
        (q.max == null ? max == null : q.max.equals(max)) &&
        minInclusive == q.minInclusive &&
        maxInclusive == q.maxInclusive &&
        precisionStep == q.precisionStep
      );
    }
    return false;
  }

  @Override
  public final int hashCode() {
    int hash = super.hashCode();
    hash += precisionStep^0x64365465;
    if (min != null) hash += min.hashCode()^0x14fa55fb;
    if (max != null) hash += max.hashCode()^0x733fa5fe;
    return hash +
      (Boolean.valueOf(minInclusive).hashCode()^0x14fa55fb)+
      (Boolean.valueOf(maxInclusive).hashCode()^0x733fa5fe);
  }

  // members (package private, to be also fast accessible by NumericRangeTermEnum)
  final int precisionStep;
  final NumericType dataType;
  final T min, max;
  final boolean minInclusive,maxInclusive;

  // used to handle float/double infinity correcty
  static final long LONG_NEGATIVE_INFINITY =
    NumericUtils.doubleToSortableLong(Double.NEGATIVE_INFINITY);
  static final long LONG_POSITIVE_INFINITY =
    NumericUtils.doubleToSortableLong(Double.POSITIVE_INFINITY);
  static final int INT_NEGATIVE_INFINITY =
    NumericUtils.floatToSortableInt(Float.NEGATIVE_INFINITY);
  static final int INT_POSITIVE_INFINITY =
    NumericUtils.floatToSortableInt(Float.POSITIVE_INFINITY);

  /**
   * Subclass of FilteredTermsEnum for enumerating all terms that match the
   * sub-ranges for trie range queries, using flex API.
   * <p>
   * WARNING: This term enumeration is not guaranteed to be always ordered by
   * {@link Term#compareTo}.
   * The ordering depends on how {@link NumericUtils#splitLongRange} and
   * {@link NumericUtils#splitIntRange} generates the sub-ranges. For
   * {@link MultiTermQuery} ordering is not relevant.
   */
  private final class NumericRangeTermsEnum extends FilteredTermsEnum {

    private BytesRef currentLowerBound, currentUpperBound;

    private final LinkedList<BytesRef> rangeBounds = new LinkedList<>();

    NumericRangeTermsEnum(final TermsEnum tenum) {
      super(tenum);
      switch (dataType) {
        case LONG:
        case DOUBLE: {
          // lower
          long minBound;
          if (dataType == NumericType.LONG) {
            minBound = (min == null) ? Long.MIN_VALUE : min.longValue();
          } else {
            assert dataType == NumericType.DOUBLE;
            minBound = (min == null) ? LONG_NEGATIVE_INFINITY
              : NumericUtils.doubleToSortableLong(min.doubleValue());
          }
          if (!minInclusive && min != null) {
            if (minBound == Long.MAX_VALUE) break;
            minBound++;
          }
          
          // upper
          long maxBound;
          if (dataType == NumericType.LONG) {
            maxBound = (max == null) ? Long.MAX_VALUE : max.longValue();
          } else {
            assert dataType == NumericType.DOUBLE;
            maxBound = (max == null) ? LONG_POSITIVE_INFINITY
              : NumericUtils.doubleToSortableLong(max.doubleValue());
          }
          if (!maxInclusive && max != null) {
            if (maxBound == Long.MIN_VALUE) break;
            maxBound--;
          }
          
          NumericUtils.splitLongRange(new NumericUtils.LongRangeBuilder() {
            @Override
            public final void addRange(BytesRef minPrefixCoded, BytesRef maxPrefixCoded) {
              rangeBounds.add(minPrefixCoded);
              rangeBounds.add(maxPrefixCoded);
            }
          }, precisionStep, minBound, maxBound);
          break;
        }
          
        case INT:
        case FLOAT: {
          // lower
          int minBound;
          if (dataType == NumericType.INT) {
            minBound = (min == null) ? Integer.MIN_VALUE : min.intValue();
          } else {
            assert dataType == NumericType.FLOAT;
            minBound = (min == null) ? INT_NEGATIVE_INFINITY
              : NumericUtils.floatToSortableInt(min.floatValue());
          }
          if (!minInclusive && min != null) {
            if (minBound == Integer.MAX_VALUE) break;
            minBound++;
          }
          
          // upper
          int maxBound;
          if (dataType == NumericType.INT) {
            maxBound = (max == null) ? Integer.MAX_VALUE : max.intValue();
          } else {
            assert dataType == NumericType.FLOAT;
            maxBound = (max == null) ? INT_POSITIVE_INFINITY
              : NumericUtils.floatToSortableInt(max.floatValue());
          }
          if (!maxInclusive && max != null) {
            if (maxBound == Integer.MIN_VALUE) break;
            maxBound--;
          }
          
          NumericUtils.splitIntRange(new NumericUtils.IntRangeBuilder() {
            @Override
            public final void addRange(BytesRef minPrefixCoded, BytesRef maxPrefixCoded) {
              rangeBounds.add(minPrefixCoded);
              rangeBounds.add(maxPrefixCoded);
            }
          }, precisionStep, minBound, maxBound);
          break;
        }
          
        default:
          // should never happen
          throw new IllegalArgumentException("Invalid NumericType");
      }
    }
    
    private void nextRange() {
      assert rangeBounds.size() % 2 == 0;

      currentLowerBound = rangeBounds.removeFirst();
      assert currentUpperBound == null || currentUpperBound.compareTo(currentLowerBound) <= 0 :
        "The current upper bound must be <= the new lower bound";
      
      currentUpperBound = rangeBounds.removeFirst();
    }
    
    @Override
    protected final BytesRef nextSeekTerm(BytesRef term) {
      while (rangeBounds.size() >= 2) {
        nextRange();
        
        // if the new upper bound is before the term parameter, the sub-range is never a hit
        if (term != null && term.compareTo(currentUpperBound) > 0)
          continue;
        // never seek backwards, so use current term if lower bound is smaller
        return (term != null && term.compareTo(currentLowerBound) > 0) ?
          term : currentLowerBound;
      }
      
      // no more sub-range enums available
      assert rangeBounds.isEmpty();
      currentLowerBound = currentUpperBound = null;
      return null;
    }
    
    @Override
    protected final AcceptStatus accept(BytesRef term) {
      while (currentUpperBound == null || term.compareTo(currentUpperBound) > 0) {
        if (rangeBounds.isEmpty())
          return AcceptStatus.END;
        // peek next sub-range, only seek if the current term is smaller than next lower bound
        if (term.compareTo(rangeBounds.getFirst()) < 0)
          return AcceptStatus.NO_AND_SEEK;
        // step forward to next range without seeking, as next lower range bound is less or equal current term
        nextRange();
      }
      return AcceptStatus.YES;
    }

  }
  
}
