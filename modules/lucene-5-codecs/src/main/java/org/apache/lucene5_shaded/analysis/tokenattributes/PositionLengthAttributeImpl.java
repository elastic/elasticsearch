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

/** Default implementation of {@link PositionLengthAttribute}. */
public class PositionLengthAttributeImpl extends AttributeImpl implements PositionLengthAttribute, Cloneable {
  private int positionLength = 1;
  
  /** Initializes this attribute with position length of 1. */
  public PositionLengthAttributeImpl() {}
  
  @Override
  public void setPositionLength(int positionLength) {
    if (positionLength < 1) {
      throw new IllegalArgumentException
        ("Position length must be 1 or greater: got " + positionLength);
    }
    this.positionLength = positionLength;
  }

  @Override
  public int getPositionLength() {
    return positionLength;
  }

  @Override
  public void clear() {
    this.positionLength = 1;
  }
  
  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    
    if (other instanceof PositionLengthAttributeImpl) {
      PositionLengthAttributeImpl _other = (PositionLengthAttributeImpl) other;
      return positionLength ==  _other.positionLength;
    }
 
    return false;
  }

  @Override
  public int hashCode() {
    return positionLength;
  }
  
  @Override
  public void copyTo(AttributeImpl target) {
    PositionLengthAttribute t = (PositionLengthAttribute) target;
    t.setPositionLength(positionLength);
  }  

  @Override
  public void reflectWith(AttributeReflector reflector) {
    reflector.reflect(PositionLengthAttribute.class, "positionLength", positionLength);
  }
}
