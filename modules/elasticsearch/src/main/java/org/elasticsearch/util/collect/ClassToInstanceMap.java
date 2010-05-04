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

import javax.annotation.Nullable;
import java.util.Map;

/**
 * A map, each entry of which maps a Java
 * <a href="http://tinyurl.com/2cmwkz">raw type</a> to an instance of that type.
 * In addition to implementing {@code Map}, the additional type-safe operations
 * {@link #putInstance} and {@link #getInstance} are available.
 *
 * <p>Like any other {@code Map<Class, Object>}, this map may contain entries
 * for primitive types, and a primitive type and its corresponding wrapper type
 * may map to different values.
 *
 * @param <B> the common supertype that all entries must share; often this is
 *     simply {@link Object}
 *
 * @author Kevin Bourrillion
 */
@GwtCompatible
public interface ClassToInstanceMap<B> extends Map<Class<? extends B>, B> {
  /**
   * Returns the value the specified class is mapped to, or {@code null} if no
   * entry for this class is present. This will only return a value that was
   * bound to this specific class, not a value that may have been bound to a
   * subtype.
   */
  <T extends B> T getInstance(Class<T> type);

  /**
   * Maps the specified class to the specified value. Does <i>not</i> associate
   * this value with any of the class's supertypes.
   *
   * @return the value previously associated with this class (possibly {@code
   *     null}), or {@code null} if there was no previous entry.
   */
  <T extends B> T putInstance(Class<T> type, @Nullable T value);
}
