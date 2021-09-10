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

import org.apache.lucene5_shaded.index.IndexReader;
import org.apache.lucene5_shaded.search.MultiTermQuery.RewriteMethod;

/**
 * Rewrites MultiTermQueries into a filter, using DocTermOrds for term enumeration.
 * <p>
 * This can be used to perform these queries against an unindexed docvalues field.
 * @lucene.experimental
 * @deprecated Use {@link DocValuesRewriteMethod} instead.
 */
@Deprecated
public final class DocTermOrdsRewriteMethod extends RewriteMethod {
  
  private final DocValuesRewriteMethod rewriteMethod = new DocValuesRewriteMethod();
  
  @Override
  public Query rewrite(IndexReader reader, MultiTermQuery query) {
    return rewriteMethod.rewrite(reader, query);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    return 877;
  }
}
