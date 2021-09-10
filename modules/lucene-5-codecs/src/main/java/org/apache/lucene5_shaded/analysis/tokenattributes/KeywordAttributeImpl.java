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
package org.apache.lucene5_shaded.analysis.tokenattributes;


import org.apache.lucene5_shaded.util.AttributeImpl;
import org.apache.lucene5_shaded.util.AttributeReflector;

/** Default implementation of {@link KeywordAttribute}. */
public final class KeywordAttributeImpl extends AttributeImpl implements
    KeywordAttribute {
  private boolean keyword;
  
  /** Initialize this attribute with the keyword value as false. */
  public KeywordAttributeImpl() {}

  @Override
  public void clear() {
    keyword = false;
  }

  @Override
  public void copyTo(AttributeImpl target) {
    KeywordAttribute attr = (KeywordAttribute) target;
    attr.setKeyword(keyword);
  }

  @Override
  public int hashCode() {
    return keyword ? 31 : 37;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (getClass() != obj.getClass())
      return false;
    final KeywordAttributeImpl other = (KeywordAttributeImpl) obj;
    return keyword == other.keyword;
  }

  @Override
  public boolean isKeyword() {
    return keyword;
  }

  @Override
  public void setKeyword(boolean isKeyword) {
    keyword = isKeyword;
  }

  @Override
  public void reflectWith(AttributeReflector reflector) {
    reflector.reflect(KeywordAttribute.class, "keyword", keyword);
  }
}
