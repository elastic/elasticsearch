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
import java.util.Objects;

import org.apache.lucene5_shaded.index.LeafReader;
import org.apache.lucene5_shaded.index.LeafReaderContext;
import org.apache.lucene5_shaded.util.Bits;
import org.apache.lucene5_shaded.util.ToStringUtils;

/**
 * A {@link Query} that matches documents that have a value for a given field
 * as reported by {@link LeafReader#getDocsWithField(String)}.
 */
public final class FieldValueQuery extends Query {

  private final String field;

  /** Create a query that will match that have a value for the given
   *  {@code field}. */
  public FieldValueQuery(String field) {
    this.field = Objects.requireNonNull(field);
  }

  @Override
  public boolean equals(Object obj) {
    if (super.equals(obj) == false) {
      return false;
    }
    final FieldValueQuery that = (FieldValueQuery) obj;
    return field.equals(that.field);
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + field.hashCode();
  }

  @Override
  public String toString(String field) {
    return "FieldValueQuery [field=" + this.field + "]" + ToStringUtils.boost(getBoost());
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new RandomAccessWeight(this) {

      @Override
      protected Bits getMatchingDocs(LeafReaderContext context) throws IOException {
        return context.reader().getDocsWithField(field);
      }

    };
  }

}
