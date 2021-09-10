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


import org.apache.lucene5_shaded.util.Attribute;
import org.apache.lucene5_shaded.util.AttributeSource; // javadocs only
import org.apache.lucene5_shaded.util.BytesRef;
import org.apache.lucene5_shaded.index.Terms; // javadocs only

/** Add this {@link Attribute} to a fresh {@link AttributeSource} before calling
 * {@link MultiTermQuery#getTermsEnum(Terms,AttributeSource)}.
 * {@link FuzzyQuery} is using this to control its internal behaviour
 * to only return competitive terms.
 * <p><b>Please note:</b> This attribute is intended to be added by the {@link MultiTermQuery.RewriteMethod}
 * to an empty {@link AttributeSource} that is shared for all segments
 * during query rewrite. This attribute source is passed to all segment enums
 * on {@link MultiTermQuery#getTermsEnum(Terms,AttributeSource)}.
 * {@link TopTermsRewrite} uses this attribute to
 * inform all enums about the current boost, that is not competitive.
 * @lucene.internal
 */
public interface MaxNonCompetitiveBoostAttribute extends Attribute {
  /** This is the maximum boost that would not be competitive. */
  public void setMaxNonCompetitiveBoost(float maxNonCompetitiveBoost);
  /** This is the maximum boost that would not be competitive. Default is negative infinity, which means every term is competitive. */
  public float getMaxNonCompetitiveBoost();
  /** This is the term or <code>null</code> of the term that triggered the boost change. */
  public void setCompetitiveTerm(BytesRef competitiveTerm);
  /** This is the term or <code>null</code> of the term that triggered the boost change. Default is <code>null</code>, which means every term is competitoive. */
  public BytesRef getCompetitiveTerm();
}
