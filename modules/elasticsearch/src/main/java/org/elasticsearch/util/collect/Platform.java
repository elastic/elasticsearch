/*
 * Copyright (C) 2008 Google Inc.
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
import org.elasticsearch.util.annotations.GwtIncompatible;

import java.lang.reflect.Array;
import java.util.List;

/**
 * Methods factored out so that they can be emulated differently in GWT.
 *
 * @author Hayward Chan
 */
@GwtCompatible(emulated = true)
class Platform {

  /**
   * Calls {@link List#subList(int, int)}.  Factored out so that it can be
   * emulated in GWT.
   *
   * <p>This method is not supported in GWT yet.  See <a
   * href="http://code.google.com/p/google-web-toolkit/issues/detail?id=1791">
   * GWT issue 1791</a>
   */
  @GwtIncompatible("List.subList")
  static <T> List<T> subList(List<T> list, int fromIndex, int toIndex) {
    return list.subList(fromIndex, toIndex);
  }

  /**
   * Calls {@link Class#isInstance(Object)}.  Factored out so that it can be
   * emulated in GWT.
   */
  @GwtIncompatible("Class.isInstance")
  static boolean isInstance(Class<?> clazz, Object obj) {
    return clazz.isInstance(obj);
  }

  /**
   * Clone the given array using {@link Object#clone()}.  It is factored out so
   * that it can be emulated in GWT.
   */
  static <T> T[] clone(T[] array) {
    return array.clone();
  }

  /**
   * Returns a new array of the given length with the specified component type.
   *
   * @param type the component type
   * @param length the length of the new array
   */
  @GwtIncompatible("Array.newInstance(Class, int)")
  @SuppressWarnings("unchecked")
  static <T> T[] newArray(Class<T> type, int length) {
    return (T[]) Array.newInstance(type, length);
  }

  /**
   * Returns a new array of the given length with the same type as a reference
   * array.
   *
   * @param reference any array of the desired type
   * @param length the length of the new array
   */
  static <T> T[] newArray(T[] reference, int length) {
    Class<?> type = reference.getClass().getComponentType();

    // the cast is safe because
    // result.getClass() == reference.getClass().getComponentType()
    @SuppressWarnings("unchecked")
    T[] result = (T[]) Array.newInstance(type, length);
    return result;
  }

  private Platform() {}
}