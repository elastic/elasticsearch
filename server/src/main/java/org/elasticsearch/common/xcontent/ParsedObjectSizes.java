/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;
import static org.apache.lucene.util.RamUsageEstimator.alignObjectSize;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

/**
 * Sizes of what parsing a request creates, for {@link SizedParsedObject}: strings, {@link BytesRef}s, boxed primitives, and collections
 * sized from how they grow when filled one element at a time. Sizes follow the running JVM's layout as {@link RamUsageEstimator} works it
 * out, and assume compact strings (the default), which store Latin-1 text at one byte per character.
 */
public final class ParsedObjectSizes {

    private static final long STRING = shallowSizeOfInstance(String.class);
    private static final long BYTES_REF = shallowSizeOfInstance(BytesRef.class);
    private static final long ARRAY_LIST = shallowSizeOfInstance(ArrayList.class);
    private static final long HASH_SET = shallowSizeOfInstance(HashSet.class);
    private static final long HASH_MAP = shallowSizeOfInstance(HashMap.class);
    private static final long LINKED_HASH_MAP = shallowSizeOfInstance(LinkedHashMap.class);
    private static final long HASH_MAP_ENTRY = shallowSizeOfInstance(entryClass(new HashMap<>()));
    private static final long LINKED_HASH_MAP_ENTRY = shallowSizeOfInstance(entryClass(new LinkedHashMap<>()));
    private static final long INTEGER = shallowSizeOfInstance(Integer.class);
    private static final long LONG = shallowSizeOfInstance(Long.class);
    private static final long SHORT = shallowSizeOfInstance(Short.class);
    private static final long BYTE = shallowSizeOfInstance(Byte.class);
    private static final long FLOAT = shallowSizeOfInstance(Float.class);
    private static final long DOUBLE = shallowSizeOfInstance(Double.class);

    /** An {@link ArrayList} first allocates an array of this many slots, then grows it by half each time it is full. */
    private static final int ARRAY_LIST_FIRST_CAPACITY = 10;
    /** A {@link HashMap} first allocates a table of this many slots, then doubles it once entries exceed three quarters of the slots. */
    private static final int HASH_MAP_FIRST_CAPACITY = 16;

    private ParsedObjectSizes() {}

    /** The class of the entries of {@code map}, which the JDK does not make public. */
    private static Class<?> entryClass(Map<Object, Object> map) {
        map.put("", "");
        return map.entrySet().iterator().next().getClass();
    }

    /**
     * A string, at one byte per character if all are Latin-1 and two otherwise.
     */
    public static long string(@Nullable String s) {
        if (s == null) {
            return 0;
        }
        return STRING + alignObjectSize(NUM_BYTES_ARRAY_HEADER + (long) s.length() * (isLatin1(s) ? 1 : 2));
    }

    private static boolean isLatin1(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (s.charAt(i) > 0xFF) {
                return false;
            }
        }
        return true;
    }

    /**
     * A {@link BytesRef} and its whole array, which can be longer than the bytes it refers to.
     */
    public static long bytesRef(BytesRef bytesRef) {
        return BYTES_REF + RamUsageEstimator.sizeOf(bytesRef.bytes);
    }

    /**
     * An array of {@code length} references.
     */
    public static long objectArray(int length) {
        return alignObjectSize(NUM_BYTES_ARRAY_HEADER + (long) NUM_BYTES_OBJECT_REF * length);
    }

    /**
     * An {@link ArrayList} of {@code size} elements, not counting the elements, filled one at a time as parsers do. An empty list shares an
     * empty array. A list filled at once (by {@code addAll} or a copy constructor) can have a smaller array, so it may be over-counted.
     */
    public static long arrayList(int size) {
        return ARRAY_LIST + (size == 0 ? 0 : objectArray(arrayListCapacity(size)));
    }

    static int arrayListCapacity(int size) {
        int capacity = ARRAY_LIST_FIRST_CAPACITY;
        while (capacity < size) {
            capacity += capacity >> 1;
        }
        return capacity;
    }

    /**
     * A list, not counting its elements. An {@link ArrayList} is sized by {@link #arrayList}, and any other list, such as the unmodifiable
     * one {@code toList()} returns, as its own object plus an array of exactly its size.
     */
    public static long list(List<?> list) {
        if (list.getClass() == ArrayList.class) {
            return arrayList(list.size());
        }
        return shallowSizeOfInstance(list.getClass()) + objectArray(list.size());
    }

    /**
     * A {@link HashSet} of {@code size} elements, not counting the elements, filled one at a time.
     */
    public static long hashSet(int size) {
        return HASH_SET + hashMap(size, false);
    }

    /**
     * A {@link HashMap}, or a {@link LinkedHashMap} if {@code linked}, of {@code size} entries filled one at a time. Counts the map, its
     * table and an entry per key, but not the keys and values. Heavy hash collisions, which grow the table early or build tree bins, are
     * not counted.
     */
    static long hashMap(int size, boolean linked) {
        long map = linked ? LINKED_HASH_MAP : HASH_MAP;
        if (size == 0) {
            return map;
        }
        return map + objectArray(hashMapCapacity(size)) + size * (linked ? LINKED_HASH_MAP_ENTRY : HASH_MAP_ENTRY);
    }

    static int hashMapCapacity(int size) {
        int capacity = HASH_MAP_FIRST_CAPACITY;
        while (size > capacity / 4 * 3) {
            capacity <<= 1;
        }
        return capacity;
    }

    /**
     * A value as parsers return it: a string, {@link BytesRef}, boxed primitive, byte array, or the maps and lists of
     * {@code XContentParser.map()} and {@code XContentParser.list()} that hold them. Map keys are field names, which the parser charges, so
     * they are not counted. {@link SizedParsedObject#UNKNOWN} for anything else, including a map or list that holds anything else.
     */
    public static long value(@Nullable Object value) {
        if (value == null || value instanceof Boolean) {
            // Booleans are shared constants.
            return 0;
        }
        if (value instanceof String s) {
            return string(s);
        }
        if (value instanceof BytesRef bytesRef) {
            return bytesRef(bytesRef);
        }
        if (value instanceof byte[] bytes) {
            return RamUsageEstimator.sizeOf(bytes);
        }
        final long boxed = boxed(value);
        if (boxed != SizedParsedObject.UNKNOWN) {
            return boxed;
        }
        if (value.getClass() == HashMap.class || value.getClass() == LinkedHashMap.class) {
            final Map<?, ?> map = (Map<?, ?>) value;
            long size = hashMap(map.size(), value.getClass() == LinkedHashMap.class);
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                size = add(size, value(entry.getValue()));
                if (size == SizedParsedObject.UNKNOWN) {
                    return size;
                }
            }
            return size;
        }
        if (value.getClass() == ArrayList.class) {
            final List<?> list = (List<?>) value;
            long size = arrayList(list.size());
            for (Object element : list) {
                size = add(size, value(element));
                if (size == SizedParsedObject.UNKNOWN) {
                    return size;
                }
            }
            return size;
        }
        return SizedParsedObject.UNKNOWN;
    }

    /**
     * A boxed primitive number, 0 for {@code null}, or {@link SizedParsedObject#UNKNOWN} for anything else, such as a {@code BigInteger}.
     * Small integers may be shared cached instances but are still counted.
     */
    public static long boxed(@Nullable Object number) {
        if (number == null) {
            return 0;
        }
        if (number instanceof Integer) {
            return INTEGER;
        }
        if (number instanceof Long) {
            return LONG;
        }
        if (number instanceof Float) {
            return FLOAT;
        }
        if (number instanceof Double) {
            return DOUBLE;
        }
        if (number instanceof Short) {
            return SHORT;
        }
        if (number instanceof Byte) {
            return BYTE;
        }
        return SizedParsedObject.UNKNOWN;
    }

    /**
     * The sum of two sizes, or {@link SizedParsedObject#UNKNOWN} if either is negative.
     */
    public static long add(long a, long b) {
        return a < 0 || b < 0 ? SizedParsedObject.UNKNOWN : a + b;
    }
}
