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
import org.elasticsearch.util.annotations.GwtIncompatible;

import javax.annotation.Nullable;
import java.util.Collection;

/**
 * Static utility methods pertaining to object arrays.
 *
 * @author Kevin Bourrillion
 */
@GwtCompatible
public final class ObjectArrays {
  private ObjectArrays() {}

  /**
   * Returns a new array of the given length with the specified component type.
   *
   * @param type the component type
   * @param length the length of the new array
   */
  @GwtIncompatible("Array.newInstance(Class, int)")
  @SuppressWarnings("unchecked")
  public static <T> T[] newArray(Class<T> type, int length) {
    return Platform.newArray(type, length);
  }

  /**
   * Returns a new array of the given length with the same type as a reference
   * array.
   *
   * @param reference any array of the desired type
   * @param length the length of the new array
   */
  public static <T> T[] newArray(T[] reference, int length) {
    return Platform.newArray(reference, length);
  }

  /**
   * Returns a new array that contains the concatenated contents of two arrays.
   *
   * @param first the first array of elements to concatenate
   * @param second the second array of elements to concatenate
   * @param type the component type of the returned array
   */
  @GwtIncompatible("Array.newInstance(Class, int)")
  public static <T> T[] concat(T[] first, T[] second, Class<T> type) {
    T[] result = newArray(type, first.length + second.length);
    System.arraycopy(first, 0, result, 0, first.length);
    System.arraycopy(second, 0, result, first.length, second.length);
    return result;
  }

  /**
   * Returns a new array that prepends {@code element} to {@code array}.
   *
   * @param element the element to prepend to the front of {@code array}
   * @param array the array of elements to append
   * @return an array whose size is one larger than {@code array}, with
   *     {@code element} occupying the first position, and the
   *     elements of {@code array} occupying the remaining elements.
   */
  public static <T> T[] concat(@Nullable T element, T[] array) {
    T[] result = newArray(array, array.length + 1);
    result[0] = element;
    System.arraycopy(array, 0, result, 1, array.length);
    return result;
  }

  /**
   * Returns a new array that appends {@code element} to {@code array}.
   *
   * @param array the array of elements to prepend
   * @param element the element to append to the end
   * @return an array whose size is one larger than {@code array}, with
   *     the same contents as {@code array}, plus {@code element} occupying the
   *     last position.
   */
  public static <T> T[] concat(T[] array, @Nullable T element) {
    T[] result = arraysCopyOf(array, array.length + 1);
    result[array.length] = element;
    return result;
  }

  /** GWT safe version of Arrays.copyOf. */
  private static <T> T[] arraysCopyOf(T[] original, int newLength) {
    T[] copy = newArray(original, newLength);
    System.arraycopy(
        original, 0, copy, 0, Math.min(original.length, newLength));
    return copy;
  }

  /**
   * Returns an array containing all of the elements in the specified
   * collection; the runtime type of the returned array is that of the specified
   * array. If the collection fits in the specified array, it is returned
   * therein. Otherwise, a new array is allocated with the runtime type of the
   * specified array and the size of the specified collection.
   *
   * <p>If the collection fits in the specified array with room to spare (i.e.,
   * the array has more elements than the collection), the element in the array
   * immediately following the end of the collection is set to null. This is
   * useful in determining the length of the collection <i>only</i> if the
   * caller knows that the collection does not contain any null elements.
   *
   * <p>This method returns the elements in the order they are returned by the
   * collection's iterator.
   *
   * <p>TODO: Support concurrent collections whose size can change while the
   * method is running.
   *
   * @param c the collection for which to return an array of elements
   * @param array the array in which to place the collection elements
   * @throws ArrayStoreException if the runtime type of the specified array is
   *     not a supertype of the runtime type of every element in the specified
   *     collection
   */
  static <T> T[] toArrayImpl(Collection<?> c, T[] array) {
    int size = c.size();
    if (array.length < size) {
      array = newArray(array, size);
    }
    fillArray(c, array);
    if (array.length > size) {
      array[size] = null;
    }
    return array;
  }

  /**
   * Returns an array containing all of the elements in the specified
   * collection. This method returns the elements in the order they are returned
   * by the collection's iterator. The returned array is "safe" in that no
   * references to it are maintained by the collection. The caller is thus free
   * to modify the returned array.
   *
   * <p>This method assumes that the collection size doesn't change while the
   * method is running.
   *
   * <p>TODO: Support concurrent collections whose size can change while the
   * method is running.
   *
   * @param c the collection for which to return an array of elements
   */
  static Object[] toArrayImpl(Collection<?> c) {
    return fillArray(c, new Object[c.size()]);
  }

  private static Object[] fillArray(Iterable<?> elements, Object[] array) {
    int i = 0;
    for (Object element : elements) {
      array[i++] = element;
    }
    return array;
  }
}
