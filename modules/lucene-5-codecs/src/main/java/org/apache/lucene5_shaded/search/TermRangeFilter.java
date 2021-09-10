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

import org.apache.lucene5_shaded.util.BytesRef;

/**
 * A Filter that restricts search results to a range of term
 * values in a given field.
 *
 * <p>This filter matches the documents looking for terms that fall into the
 * supplied range according to {@link
 * Byte#compareTo(Byte)},  It is not intended
 * for numerical ranges; use {@link NumericRangeFilter} instead.
 * @since 2.9
 * @deprecated Use {@link TermRangeQuery} and {@link BooleanClause.Occur#FILTER} clauses instead.
 */
@Deprecated
public class TermRangeFilter extends MultiTermQueryWrapperFilter<TermRangeQuery> {
    
  /**
   * @param fieldName The field this range applies to
   * @param lowerTerm The lower bound on this range
   * @param upperTerm The upper bound on this range
   * @param includeLower Does this range include the lower bound?
   * @param includeUpper Does this range include the upper bound?
   * @throws IllegalArgumentException if both terms are null or if
   *  lowerTerm is null and includeLower is true (similar for upperTerm
   *  and includeUpper)
   */
  public TermRangeFilter(String fieldName, BytesRef lowerTerm, BytesRef upperTerm,
                     boolean includeLower, boolean includeUpper) {
      super(new TermRangeQuery(fieldName, lowerTerm, upperTerm, includeLower, includeUpper));
  }

  /**
   * Factory that creates a new TermRangeFilter using Strings for term text.
   */
  public static TermRangeFilter newStringRange(String field, String lowerTerm, String upperTerm, boolean includeLower, boolean includeUpper) {
    BytesRef lower = lowerTerm == null ? null : new BytesRef(lowerTerm);
    BytesRef upper = upperTerm == null ? null : new BytesRef(upperTerm);
    return new TermRangeFilter(field, lower, upper, includeLower, includeUpper);
  }
  
  /**
   * Constructs a filter for field <code>fieldName</code> matching
   * less than or equal to <code>upperTerm</code>.
   */
  public static TermRangeFilter Less(String fieldName, BytesRef upperTerm) {
      return new TermRangeFilter(fieldName, null, upperTerm, false, true);
  }

  /**
   * Constructs a filter for field <code>fieldName</code> matching
   * greater than or equal to <code>lowerTerm</code>.
   */
  public static TermRangeFilter More(String fieldName, BytesRef lowerTerm) {
      return new TermRangeFilter(fieldName, lowerTerm, null, true, false);
  }
  
  /** Returns the lower value of this range filter */
  public BytesRef getLowerTerm() { return query.getLowerTerm(); }

  /** Returns the upper value of this range filter */
  public BytesRef getUpperTerm() { return query.getUpperTerm(); }
  
  /** Returns <code>true</code> if the lower endpoint is inclusive */
  public boolean includesLower() { return query.includesLower(); }
  
  /** Returns <code>true</code> if the upper endpoint is inclusive */
  public boolean includesUpper() { return query.includesUpper(); }
}
