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


import org.apache.lucene5_shaded.util.AttributeImpl;
import org.apache.lucene5_shaded.util.AttributeReflector;
import org.apache.lucene5_shaded.util.BytesRef;

/** Implementation class for {@link MaxNonCompetitiveBoostAttribute}.
 * @lucene.internal
 */
public final class MaxNonCompetitiveBoostAttributeImpl extends AttributeImpl implements MaxNonCompetitiveBoostAttribute {
  private float maxNonCompetitiveBoost = Float.NEGATIVE_INFINITY;
  private BytesRef competitiveTerm = null;

  @Override
  public void setMaxNonCompetitiveBoost(final float maxNonCompetitiveBoost) {
    this.maxNonCompetitiveBoost = maxNonCompetitiveBoost;
  }
  
  @Override
  public float getMaxNonCompetitiveBoost() {
    return maxNonCompetitiveBoost;
  }

  @Override
  public void setCompetitiveTerm(final BytesRef competitiveTerm) {
    this.competitiveTerm = competitiveTerm;
  }
  
  @Override
  public BytesRef getCompetitiveTerm() {
    return competitiveTerm;
  }

  @Override
  public void clear() {
    maxNonCompetitiveBoost = Float.NEGATIVE_INFINITY;
    competitiveTerm = null;
  }
  
  @Override
  public void copyTo(AttributeImpl target) {
    final MaxNonCompetitiveBoostAttributeImpl t = (MaxNonCompetitiveBoostAttributeImpl) target;
    t.setMaxNonCompetitiveBoost(maxNonCompetitiveBoost);
    t.setCompetitiveTerm(competitiveTerm);
  }

  @Override
  public void reflectWith(AttributeReflector reflector) {
    reflector.reflect(MaxNonCompetitiveBoostAttribute.class, "maxNonCompetitiveBoost", maxNonCompetitiveBoost);
    reflector.reflect(MaxNonCompetitiveBoostAttribute.class, "competitiveTerm", competitiveTerm);
  }
}
