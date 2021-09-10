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

/** Default implementation of {@link OffsetAttribute}. */
public class OffsetAttributeImpl extends AttributeImpl implements OffsetAttribute, Cloneable {
  private int startOffset;
  private int endOffset;
  
  /** Initialize this attribute with startOffset and endOffset of 0. */
  public OffsetAttributeImpl() {}

  @Override
  public int startOffset() {
    return startOffset;
  }

  @Override
  public void setOffset(int startOffset, int endOffset) {

    // TODO: we could assert that this is set-once, ie,
    // current values are -1?  Very few token filters should
    // change offsets once set by the tokenizer... and
    // tokenizer should call clearAtts before re-using
    // OffsetAtt

    if (startOffset < 0 || endOffset < startOffset) {
      throw new IllegalArgumentException("startOffset must be non-negative, and endOffset must be >= startOffset, "
          + "startOffset=" + startOffset + ",endOffset=" + endOffset);
    }

    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }
  
  @Override
  public int endOffset() {
    return endOffset;
  }


  @Override
  public void clear() {
    // TODO: we could use -1 as default here?  Then we can
    // assert in setOffset...
    startOffset = 0;
    endOffset = 0;
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    
    if (other instanceof OffsetAttributeImpl) {
      OffsetAttributeImpl o = (OffsetAttributeImpl) other;
      return o.startOffset == startOffset && o.endOffset == endOffset;
    }
    
    return false;
  }

  @Override
  public int hashCode() {
    int code = startOffset;
    code = code * 31 + endOffset;
    return code;
  } 
  
  @Override
  public void copyTo(AttributeImpl target) {
    OffsetAttribute t = (OffsetAttribute) target;
    t.setOffset(startOffset, endOffset);
  }  

  @Override
  public void reflectWith(AttributeReflector reflector) {
    reflector.reflect(OffsetAttribute.class, "startOffset", startOffset);
    reflector.reflect(OffsetAttribute.class, "endOffset", endOffset);
  }
}
