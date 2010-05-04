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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;

/**
 * Provides static methods for serializing collection classes.
 *
 * <p>This class assists the implementation of collection classes. Do not use
 * this class to serialize collections that are defined elsewhere.
 *
 * @author Jared Levy
 */
@GwtCompatible(emulated = true) // Accessible but not supported in GWT.
final class Serialization {
  private Serialization() {}

  /**
   * Reads a count corresponding to a serialized map, multiset, or multimap. It
   * returns the size of a map serialized by {@link
   * #writeMap(Map, ObjectOutputStream)}, the number of distinct elements in a
   * multiset serialized by {@link
   * #writeMultiset(Multiset, ObjectOutputStream)}, or the number of distinct
   * keys in a multimap serialized by {@link
   * #writeMultimap(Multimap, ObjectOutputStream)}.
   *
   * <p>The returned count may be used to construct an empty collection of the
   * appropriate capacity before calling any of the {@code populate} methods.
   */
  @GwtIncompatible("java.io.ObjectInputStream")
  public static int readCount(ObjectInputStream stream) throws IOException {
    return stream.readInt();
  }

  /**
   * Stores the contents of a map in an output stream, as part of serialization.
   * It does not support concurrent maps whose content may change while the
   * method is running.
   *
   * <p>The serialized output consists of the number of entries, first key,
   * first value, second key, second value, and so on.
   */
  @GwtIncompatible("java.io.ObjectOutputStream")
  public static <K, V> void writeMap(Map<K, V> map, ObjectOutputStream stream)
      throws IOException {
    stream.writeInt(map.size());
    for (Map.Entry<K, V> entry : map.entrySet()) {
      stream.writeObject(entry.getKey());
      stream.writeObject(entry.getValue());
    }
  }

  /**
   * Populates a map by reading an input stream, as part of deserialization.
   * See {@link #writeMap} for the data format.
   */
  @GwtIncompatible("java.io.ObjectInputStream")
  public static <K, V> void populateMap(Map<K, V> map, ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    int size = stream.readInt();
    populateMap(map, stream, size);
  }

  /**
   * Populates a map by reading an input stream, as part of deserialization.
   * See {@link #writeMap} for the data format. The size is determined by a
   * prior call to {@link #readCount}.
   */
  @GwtIncompatible("java.io.ObjectInputStream")
  public static <K, V> void populateMap(Map<K, V> map, ObjectInputStream stream,
      int size) throws IOException, ClassNotFoundException {
    for (int i = 0; i < size; i++) {
      @SuppressWarnings("unchecked") // reading data stored by writeMap
      K key = (K) stream.readObject();
      @SuppressWarnings("unchecked") // reading data stored by writeMap
      V value = (V) stream.readObject();
      map.put(key, value);
    }
  }

  /**
   * Stores the contents of a multiset in an output stream, as part of
   * serialization. It does not support concurrent multisets whose content may
   * change while the method is running.
   *
   * <p>The serialized output consists of the number of distinct elements, the
   * first element, its count, the second element, its count, and so on.
   */
  @GwtIncompatible("java.io.ObjectOutputStream")
  public static <E> void writeMultiset(
      Multiset<E> multiset, ObjectOutputStream stream) throws IOException {
    int entryCount = multiset.entrySet().size();
    stream.writeInt(entryCount);
    for (Multiset.Entry<E> entry : multiset.entrySet()) {
      stream.writeObject(entry.getElement());
      stream.writeInt(entry.getCount());
    }
  }

  /**
   * Populates a multiset by reading an input stream, as part of
   * deserialization. See {@link #writeMultiset} for the data format.
   */
  @GwtIncompatible("java.io.ObjectInputStream")
  public static <E> void populateMultiset(
      Multiset<E> multiset, ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    int distinctElements = stream.readInt();
    populateMultiset(multiset, stream, distinctElements);
  }

  /**
   * Populates a multiset by reading an input stream, as part of
   * deserialization. See {@link #writeMultiset} for the data format. The number
   * of distinct elements is determined by a prior call to {@link #readCount}.
   */
  @GwtIncompatible("java.io.ObjectInputStream")
  public static <E> void populateMultiset(
      Multiset<E> multiset, ObjectInputStream stream, int distinctElements)
      throws IOException, ClassNotFoundException {
    for (int i = 0; i < distinctElements; i++) {
      @SuppressWarnings("unchecked") // reading data stored by writeMultiset
      E element = (E) stream.readObject();
      int count = stream.readInt();
      multiset.add(element, count);
    }
  }

  /**
   * Stores the contents of a multimap in an output stream, as part of
   * serialization. It does not support concurrent multimaps whose content may
   * change while the method is running. The {@link Multimap#asMap} view
   * determines the ordering in which data is written to the stream.
   *
   * <p>The serialized output consists of the number of distinct keys, and then
   * for each distinct key: the key, the number of values for that key, and the
   * key's values.
   */
  @GwtIncompatible("java.io.ObjectOutputStream")
  public static <K, V> void writeMultimap(
      Multimap<K, V> multimap, ObjectOutputStream stream) throws IOException {
    stream.writeInt(multimap.asMap().size());
    for (Map.Entry<K, Collection<V>> entry : multimap.asMap().entrySet()) {
      stream.writeObject(entry.getKey());
      stream.writeInt(entry.getValue().size());
      for (V value : entry.getValue()) {
        stream.writeObject(value);
      }
    }
  }

  /**
   * Populates a multimap by reading an input stream, as part of
   * deserialization. See {@link #writeMultimap} for the data format.
   */
  @GwtIncompatible("java.io.ObjectInputStream")
  public static <K, V> void populateMultimap(
      Multimap<K, V> multimap, ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    int distinctKeys = stream.readInt();
    populateMultimap(multimap, stream, distinctKeys);
  }

  /**
   * Populates a multimap by reading an input stream, as part of
   * deserialization. See {@link #writeMultimap} for the data format. The number
   * of distinct keys is determined by a prior call to {@link #readCount}.
   */
  @GwtIncompatible("java.io.ObjectInputStream")
  public static <K, V> void populateMultimap(
      Multimap<K, V> multimap, ObjectInputStream stream, int distinctKeys)
      throws IOException, ClassNotFoundException {
    for (int i = 0; i < distinctKeys; i++) {
      @SuppressWarnings("unchecked") // reading data stored by writeMultimap
      K key = (K) stream.readObject();
      Collection<V> values = multimap.get(key);
      int valueCount = stream.readInt();
      for (int j = 0; j < valueCount; j++) {
        @SuppressWarnings("unchecked") // reading data stored by writeMultimap
        V value = (V) stream.readObject();
        values.add(value);
      }
    }
  }

  // Secret sauce for setting final fields; don't make it public.
  @GwtIncompatible("java.lang.reflect.Field")
  static <T> FieldSetter<T> getFieldSetter(
      final Class<T> clazz, String fieldName) {
    try {
      Field field = clazz.getDeclaredField(fieldName);
      return new FieldSetter<T>(field);
    } catch (NoSuchFieldException e) {
      throw new AssertionError(e); // programmer error
    }
  }

  // Secret sauce for setting final fields; don't make it public.
  @GwtCompatible(emulated = true) // Accessible but not supported in GWT.
  static final class FieldSetter<T> {
    private final Field field;

    private FieldSetter(Field field) {
      this.field = field;
      field.setAccessible(true);
    }

    @GwtIncompatible("java.lang.reflect.Field")
    void set(T instance, Object value) {
      try {
        field.set(instance, value);
      } catch (IllegalAccessException impossible) {
        throw new AssertionError(impossible);
      }
    }

    @GwtIncompatible("java.lang.reflect.Field")
    void set(T instance, int value) {
      try {
        field.set(instance, value);
      } catch (IllegalAccessException impossible) {
        throw new AssertionError(impossible);
      }
    }
  }
}
