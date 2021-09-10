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

/** Default implementation of the common attributes used by Lucene:<ul>
 * <li>{@link CharTermAttribute}
 * <li>{@link TypeAttribute}
 * <li>{@link PositionIncrementAttribute}
 * <li>{@link PositionLengthAttribute}
 * <li>{@link OffsetAttribute}
 * </ul>*/
public class PackedTokenAttributeImpl extends CharTermAttributeImpl 
                   implements TypeAttribute, PositionIncrementAttribute,
                              PositionLengthAttribute, OffsetAttribute {

  private int startOffset,endOffset;
  private String type = DEFAULT_TYPE;
  private int positionIncrement = 1;
  private int positionLength = 1;

  /** Constructs the attribute implementation. */
  public PackedTokenAttributeImpl() {
  }

  /**
   * {@inheritDoc}
   * @see PositionIncrementAttribute
   */
  @Override
  public void setPositionIncrement(int positionIncrement) {
    if (positionIncrement < 0)
      throw new IllegalArgumentException
        ("Increment must be zero or greater: " + positionIncrement);
    this.positionIncrement = positionIncrement;
  }

  /**
   * {@inheritDoc}
   * @see PositionIncrementAttribute
   */
  @Override
  public int getPositionIncrement() {
    return positionIncrement;
  }

  /**
   * {@inheritDoc}
   * @see PositionLengthAttribute
   */
  @Override
  public void setPositionLength(int positionLength) {
    this.positionLength = positionLength;
  }

  /**
   * {@inheritDoc}
   * @see PositionLengthAttribute
   */
  @Override
  public int getPositionLength() {
    return positionLength;
  }

  /**
   * {@inheritDoc}
   * @see OffsetAttribute
   */
  @Override
  public final int startOffset() {
    return startOffset;
  }

  /**
   * {@inheritDoc}
   * @see OffsetAttribute
   */
  @Override
  public final int endOffset() {
    return endOffset;
  }

  /**
   * {@inheritDoc}
   * @see OffsetAttribute
   */
  @Override
  public void setOffset(int startOffset, int endOffset) {
    if (startOffset < 0 || endOffset < startOffset) {
      throw new IllegalArgumentException("startOffset must be non-negative, and endOffset must be >= startOffset, "
          + "startOffset=" + startOffset + ",endOffset=" + endOffset);
    }
    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

  /**
   * {@inheritDoc}
   * @see TypeAttribute
   */
  @Override
  public final String type() {
    return type;
  }

  /**
   * {@inheritDoc}
   * @see TypeAttribute
   */
  @Override
  public final void setType(String type) {
    this.type = type;
  }

  /** Resets the attributes
   */
  @Override
  public void clear() {
    super.clear();
    positionIncrement = positionLength = 1;
    startOffset = endOffset = 0;
    type = DEFAULT_TYPE;
  }
  
  /** Resets the attributes at end
   */
  @Override
  public void end() {
    super.end();
    positionIncrement = 0;
    positionLength = 1;
    startOffset = endOffset = 0;
    type = DEFAULT_TYPE;
  }

  @Override
  public PackedTokenAttributeImpl clone() {
    return (PackedTokenAttributeImpl) super.clone();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this)
      return true;

    if (obj instanceof PackedTokenAttributeImpl) {
      final PackedTokenAttributeImpl other = (PackedTokenAttributeImpl) obj;
      return (startOffset == other.startOffset &&
          endOffset == other.endOffset && 
          positionIncrement == other.positionIncrement &&
          positionLength == other.positionLength &&
          (type == null ? other.type == null : type.equals(other.type)) &&
          super.equals(obj)
      );
    } else
      return false;
  }

  @Override
  public int hashCode() {
    int code = super.hashCode();
    code = code * 31 + startOffset;
    code = code * 31 + endOffset;
    code = code * 31 + positionIncrement;
    code = code * 31 + positionLength;
    if (type != null)
      code = code * 31 + type.hashCode();
    return code;
  }

  @Override
  public void copyTo(AttributeImpl target) {
    if (target instanceof PackedTokenAttributeImpl) {
      final PackedTokenAttributeImpl to = (PackedTokenAttributeImpl) target;
      to.copyBuffer(buffer(), 0, length());
      to.positionIncrement = positionIncrement;
      to.positionLength = positionLength;
      to.startOffset = startOffset;
      to.endOffset = endOffset;
      to.type = type;
    } else {
      super.copyTo(target);
      ((OffsetAttribute) target).setOffset(startOffset, endOffset);
      ((PositionIncrementAttribute) target).setPositionIncrement(positionIncrement);
      ((PositionLengthAttribute) target).setPositionLength(positionLength);
      ((TypeAttribute) target).setType(type);
    }
  }

  @Override
  public void reflectWith(AttributeReflector reflector) {
    super.reflectWith(reflector);
    reflector.reflect(OffsetAttribute.class, "startOffset", startOffset);
    reflector.reflect(OffsetAttribute.class, "endOffset", endOffset);
    reflector.reflect(PositionIncrementAttribute.class, "positionIncrement", positionIncrement);
    reflector.reflect(PositionLengthAttribute.class, "positionLength", positionLength);
    reflector.reflect(TypeAttribute.class, "type", type);
  }

}
