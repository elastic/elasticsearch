/*
 * Copyright (C) 2007 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.util.collect;

import org.elasticsearch.util.annotations.GwtCompatible;

import java.io.Serializable;

/** An ordering that uses the reverse of the natural order of the values. */
@GwtCompatible(serializable = true)
final class UsingToStringOrdering
    extends Ordering<Object> implements Serializable {
  static final UsingToStringOrdering INSTANCE = new UsingToStringOrdering();

  public int compare(Object left, Object right) {
    return left.toString().compareTo(right.toString());
  }

  // preserve singleton-ness, so equals() and hashCode() work correctly
  private Object readResolve() {
    return INSTANCE;
  }

  @Override public String toString() {
    return "Ordering.usingToString()";
  }

  private UsingToStringOrdering() {}

  private static final long serialVersionUID = 0;
}
