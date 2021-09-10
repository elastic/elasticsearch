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


import java.util.Objects;

import org.apache.lucene5_shaded.util.AttributeImpl;
import org.apache.lucene5_shaded.util.AttributeReflector;
import org.apache.lucene5_shaded.util.BytesRef;

/** Implementation class for {@link BytesTermAttribute}.
 * @lucene.internal
 */
public class BytesTermAttributeImpl extends AttributeImpl implements BytesTermAttribute, TermToBytesRefAttribute {
  private BytesRef bytes;

  /** Initialize this attribute with no bytes. */
  public BytesTermAttributeImpl() {}

  @Override
  public BytesRef getBytesRef() {
    return bytes;
  }

  @Override
  public void setBytesRef(BytesRef bytes) {
    this.bytes = bytes;
  }

  @Override
  public void clear() {
    this.bytes = null;
  }

  @Override
  public void copyTo(AttributeImpl target) {
    BytesTermAttributeImpl other = (BytesTermAttributeImpl) target;
    other.bytes = bytes == null ? null : BytesRef.deepCopyOf(bytes);
  }

  @Override
  public AttributeImpl clone() {
    BytesTermAttributeImpl c = (BytesTermAttributeImpl)super.clone();
    copyTo(c);
    return c;
  }

  @Override
  public void reflectWith(AttributeReflector reflector) {
    reflector.reflect(TermToBytesRefAttribute.class, "bytes", bytes);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof BytesTermAttributeImpl)) return false;
    BytesTermAttributeImpl that = (BytesTermAttributeImpl) o;
    return Objects.equals(bytes, that.bytes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(bytes);
  }
}