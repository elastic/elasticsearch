/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lucene.analysis.miscellaneous;

import org.apache.lucene.util.AttributeImpl;
import org.apache.lucene.util.AttributeReflector;

/**
 * Implementation class for {@link TokenBoostAttribute}.
 */
public final class TokenBoostAttributeImpl extends AttributeImpl implements TokenBoostAttribute {
  private float boost = 1.0f;

  @Override
  public void setBoost(float boost) {
    this.boost = boost;
  }

  @Override
  public float getBoost() {
    return boost;
  }

  @Override
  public void clear() {
    boost = 1.0f;
  }

  @Override
  public void copyTo(AttributeImpl target) {
    ((TokenBoostAttribute) target).setBoost(boost);
  }

  @Override
  public void reflectWith(AttributeReflector reflector) {
    reflector.reflect(TokenBoostAttribute.class, "tokenBoost", boost);
  }
}