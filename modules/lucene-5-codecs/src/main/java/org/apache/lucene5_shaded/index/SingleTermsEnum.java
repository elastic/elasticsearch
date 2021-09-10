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


import org.apache.lucene5_shaded.search.MultiTermQuery;  // javadocs
import org.apache.lucene5_shaded.util.BytesRef;

/**
 * Subclass of FilteredTermsEnum for enumerating a single term.
 * <p>
 * For example, this can be used by {@link MultiTermQuery}s
 * that need only visit one term, but want to preserve
 * MultiTermQuery semantics such as {@link
 * MultiTermQuery#getRewriteMethod}.
 */
public final class SingleTermsEnum extends FilteredTermsEnum {
  private final BytesRef singleRef;
  
  /**
   * Creates a new <code>SingleTermsEnum</code>.
   * <p>
   * After calling the constructor the enumeration is already pointing to the term,
   * if it exists.
   */
  public SingleTermsEnum(TermsEnum tenum, BytesRef termText) {
    super(tenum);
    singleRef = termText;
    setInitialSeekTerm(termText);
  }

  @Override
  protected AcceptStatus accept(BytesRef term) {
    return term.equals(singleRef) ? AcceptStatus.YES : AcceptStatus.END;
  }
  
}
