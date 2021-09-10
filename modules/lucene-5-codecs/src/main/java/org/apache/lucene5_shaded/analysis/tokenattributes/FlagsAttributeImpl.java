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

/** Default implementation of {@link FlagsAttribute}. */
public class FlagsAttributeImpl extends AttributeImpl implements FlagsAttribute, Cloneable {
  private int flags = 0;
  
  /** Initialize this attribute with no bits set */
  public FlagsAttributeImpl() {}
  
  @Override
  public int getFlags() {
    return flags;
  }

  @Override
  public void setFlags(int flags) {
    this.flags = flags;
  }
  
  @Override
  public void clear() {
    flags = 0;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    
    if (other instanceof FlagsAttributeImpl) {
      return ((FlagsAttributeImpl) other).flags == flags;
    }
    
    return false;
  }

  @Override
  public int hashCode() {
    return flags;
  }
  
  @Override
  public void copyTo(AttributeImpl target) {
    FlagsAttribute t = (FlagsAttribute) target;
    t.setFlags(flags);
  }

  @Override
  public void reflectWith(AttributeReflector reflector) {
    reflector.reflect(FlagsAttribute.class, "flags", flags);
  }
}
