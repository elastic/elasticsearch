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

@GwtCompatible
interface MapConstraint<K, V> {
  /**
   * Throws a suitable {@code RuntimeException} if the specified key or value is
   * illegal. Typically this is either a {@link NullPointerException}, an
   * {@link IllegalArgumentException}, or a {@link ClassCastException}, though
   * an application-specific exception class may be used if appropriate.
   */
  void checkKeyValue(@Nullable K key, @Nullable V value);

  /**
   * Returns a brief human readable description of this constraint, such as
   * "Not null".
   */
  String toString();
}
