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

/** Default implementation of {@link TypeAttribute}. */
public class TypeAttributeImpl extends AttributeImpl implements TypeAttribute, Cloneable {
  private String type;
  
  /** Initialize this attribute with {@link TypeAttribute#DEFAULT_TYPE} */
  public TypeAttributeImpl() {
    this(DEFAULT_TYPE); 
  }
  
  /** Initialize this attribute with <code>type</code> */
  public TypeAttributeImpl(String type) {
    this.type = type;
  }
  
  @Override
  public String type() {
    return type;
  }

  @Override
  public void setType(String type) {
    this.type = type;
  }

  @Override
  public void clear() {
    type = DEFAULT_TYPE;    
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }
    
    if (other instanceof TypeAttributeImpl) {
      final TypeAttributeImpl o = (TypeAttributeImpl) other;
      return (this.type == null ? o.type == null : this.type.equals(o.type));
    }
    
    return false;
  }

  @Override
  public int hashCode() {
    return (type == null) ? 0 : type.hashCode();
  }
  
  @Override
  public void copyTo(AttributeImpl target) {
    TypeAttribute t = (TypeAttribute) target;
    t.setType(type);
  }

  @Override
  public void reflectWith(AttributeReflector reflector) {
    reflector.reflect(TypeAttribute.class, "type", type);
  }
}
