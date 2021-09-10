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

import org.apache.lucene5_shaded.analysis.NumericTokenStream; // for javadocs
import org.apache.lucene5_shaded.document.DoubleField; // for javadocs
import org.apache.lucene5_shaded.document.FloatField; // for javadocs
import org.apache.lucene5_shaded.document.IntField; // for javadocs
import org.apache.lucene5_shaded.document.LongField; // for javadocs
import org.apache.lucene5_shaded.util.NumericUtils; // for javadocs

/**
 * A {@link Filter} that only accepts numeric values within
 * a specified range. To use this, you must first index the
 * numeric values using {@link IntField}, {@link
 * FloatField}, {@link LongField} or {@link DoubleField} (expert: {@link
 * NumericTokenStream}).
 *
 * <p>You create a new NumericRangeFilter with the static
 * factory methods, eg:
 *
 * <pre class="prettyprint">
 * Filter f = NumericRangeFilter.newFloatRange("weight", 0.03f, 0.10f, true, true);
 * </pre>
 *
 * accepts all documents whose float valued "weight" field
 * ranges from 0.03 to 0.10, inclusive.
 * See {@link NumericRangeQuery} for details on how Lucene
 * indexes and searches numeric valued fields.
 *
 * @since 2.9
 * @deprecated Use {@link NumericRangeQuery} and {@link BooleanClause.Occur#FILTER} clauses instead.
 **/
@Deprecated
public final class NumericRangeFilter<T extends Number> extends MultiTermQueryWrapperFilter<NumericRangeQuery<T>> {

  private NumericRangeFilter(final NumericRangeQuery<T> query) {
    super(query);
  }
  
  /**
   * Factory that creates a <code>NumericRangeFilter</code>, that filters a <code>long</code>
   * range using the given <a href="NumericRangeQuery.html#precisionStepDesc"><code>precisionStep</code></a>.
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeFilter<Long> newLongRange(final String field, final int precisionStep,
    Long min, Long max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeFilter<>(
      NumericRangeQuery.newLongRange(field, precisionStep, min, max, minInclusive, maxInclusive)
    );
  }
  
  /**
   * Factory that creates a <code>NumericRangeFilter</code>, that queries a <code>long</code>
   * range using the default <code>precisionStep</code> {@link NumericUtils#PRECISION_STEP_DEFAULT} (16).
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeFilter<Long> newLongRange(final String field,
    Long min, Long max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeFilter<>(
      NumericRangeQuery.newLongRange(field, min, max, minInclusive, maxInclusive)
    );
  }
  
  /**
   * Factory that creates a <code>NumericRangeFilter</code>, that filters a <code>int</code>
   * range using the given <a href="NumericRangeQuery.html#precisionStepDesc"><code>precisionStep</code></a>.
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeFilter<Integer> newIntRange(final String field, final int precisionStep,
    Integer min, Integer max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeFilter<>(
      NumericRangeQuery.newIntRange(field, precisionStep, min, max, minInclusive, maxInclusive)
    );
  }
  
  /**
   * Factory that creates a <code>NumericRangeFilter</code>, that queries a <code>int</code>
   * range using the default <code>precisionStep</code> {@link NumericUtils#PRECISION_STEP_DEFAULT_32} (8).
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeFilter<Integer> newIntRange(final String field,
    Integer min, Integer max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeFilter<>(
      NumericRangeQuery.newIntRange(field, min, max, minInclusive, maxInclusive)
    );
  }
  
  /**
   * Factory that creates a <code>NumericRangeFilter</code>, that filters a <code>double</code>
   * range using the given <a href="NumericRangeQuery.html#precisionStepDesc"><code>precisionStep</code></a>.
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>.
   * {@link Double#NaN} will never match a half-open range, to hit {@code NaN} use a query
   * with {@code min == max == Double.NaN}. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeFilter<Double> newDoubleRange(final String field, final int precisionStep,
    Double min, Double max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeFilter<>(
      NumericRangeQuery.newDoubleRange(field, precisionStep, min, max, minInclusive, maxInclusive)
    );
  }
  
  /**
   * Factory that creates a <code>NumericRangeFilter</code>, that queries a <code>double</code>
   * range using the default <code>precisionStep</code> {@link NumericUtils#PRECISION_STEP_DEFAULT} (16).
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>.
   * {@link Double#NaN} will never match a half-open range, to hit {@code NaN} use a query
   * with {@code min == max == Double.NaN}. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeFilter<Double> newDoubleRange(final String field,
    Double min, Double max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeFilter<>(
      NumericRangeQuery.newDoubleRange(field, min, max, minInclusive, maxInclusive)
    );
  }
  
  /**
   * Factory that creates a <code>NumericRangeFilter</code>, that filters a <code>float</code>
   * range using the given <a href="NumericRangeQuery.html#precisionStepDesc"><code>precisionStep</code></a>.
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>.
   * {@link Float#NaN} will never match a half-open range, to hit {@code NaN} use a query
   * with {@code min == max == Float.NaN}. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeFilter<Float> newFloatRange(final String field, final int precisionStep,
    Float min, Float max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeFilter<>(
      NumericRangeQuery.newFloatRange(field, precisionStep, min, max, minInclusive, maxInclusive)
    );
  }

  /**
   * Factory that creates a <code>NumericRangeFilter</code>, that queries a <code>float</code>
   * range using the default <code>precisionStep</code> {@link NumericUtils#PRECISION_STEP_DEFAULT_32} (8).
   * You can have half-open ranges (which are in fact &lt;/&le; or &gt;/&ge; queries)
   * by setting the min or max value to <code>null</code>.
   * {@link Float#NaN} will never match a half-open range, to hit {@code NaN} use a query
   * with {@code min == max == Float.NaN}. By setting inclusive to false, it will
   * match all documents excluding the bounds, with inclusive on, the boundaries are hits, too.
   */
  public static NumericRangeFilter<Float> newFloatRange(final String field,
    Float min, Float max, final boolean minInclusive, final boolean maxInclusive
  ) {
    return new NumericRangeFilter<>(
      NumericRangeQuery.newFloatRange(field, min, max, minInclusive, maxInclusive)
    );
  }

  /** Returns <code>true</code> if the lower endpoint is inclusive */
  public boolean includesMin() { return query.includesMin(); }
  
  /** Returns <code>true</code> if the upper endpoint is inclusive */
  public boolean includesMax() { return query.includesMax(); }

  /** Returns the lower value of this range filter */
  public T getMin() { return query.getMin(); }

  /** Returns the upper value of this range filter */
  public T getMax() { return query.getMax(); }
  
  /** Returns the precision step. */
  public int getPrecisionStep() { return query.getPrecisionStep(); }
  
}
