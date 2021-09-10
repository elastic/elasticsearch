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

/** Default implementation of {@link PositionIncrementAttribute}. */
public class PositionIncrementAttributeImpl extends AttributeImpl implements PositionIncrementAttribute, Cloneable {
  private int positionIncrement = 1;
  
  /** Initialize this attribute with position increment of 1 */
  public PositionIncrementAttributeImpl() {}

  @Override
  public void setPositionIncrement(int positionIncrement) {
    if (positionIncrement < 0) {
      throw new IllegalArgumentException
        ("Increment must be zero or greater: got " + positionIncrement);
    }
    this.positionIncrement = positionIncrement;
  }

  @Override
  public int getPositionIncrement() {
    return positionIncrement;
  }

  @Override
  public void clear() {
    this.positionIncrement = 1;
  }
  
  @Override
  public void end() {
    this.positionIncrement = 0;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    
    if (other instanceof PositionIncrementAttributeImpl) {
      PositionIncrementAttributeImpl _other = (PositionIncrementAttributeImpl) other;
      return positionIncrement ==  _other.positionIncrement;
    }
 
    return false;
  }

  @Override
  public int hashCode() {
    return positionIncrement;
  }
  
  @Override
  public void copyTo(AttributeImpl target) {
    PositionIncrementAttribute t = (PositionIncrementAttribute) target;
    t.setPositionIncrement(positionIncrement);
  }  

  @Override
  public void reflectWith(AttributeReflector reflector) {
    reflector.reflect(PositionIncrementAttribute.class, "positionIncrement", positionIncrement);
  }
}
