/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.common.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Utility class similar to org.elasticsearch.xpack.core.watcher.common.stats.Counters, but it is using Enum instead
 * of string to identify the counter. The serialization happens using enum ordinals similar to
 * {@link StreamOutput#writeEnum(Enum)}, which means that ordinal for existing enums should remain the same for backward
 * and forward compatibility of the serialization protocol.
 */
public class EnumCounters<E extends Enum<E>> implements Writeable {
    private final AtomicLongArray counters;
    private final E[] enums;

    public EnumCounters(Class<E> enumClass) {
        counters = new AtomicLongArray(enumClass.getEnumConstants().length);
        enums = enumClass.getEnumConstants();
    }

    public EnumCounters(StreamInput in, Class<E> enumClass) throws IOException {
        int size = in.readVInt();
        enums = enumClass.getEnumConstants();
        long[] vals = new long[enums.length];
        for (int i = 0; i < size; i++) {
            long val = in.readVLong();
            if (i < vals.length) {
                vals[i] = val;
            }
        }
        counters = new AtomicLongArray(vals);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(counters.length());
        for (int i = 0; i < counters.length(); i++) {
            out.writeVLong(counters.get(i));
        }
    }

    public void set(E name) {
        counters.set(name.ordinal(), 0);
    }

    public void inc(E name) {
        counters.incrementAndGet(name.ordinal());
    }

    public void inc(E name, long count) {
        counters.addAndGet(name.ordinal(), count);
    }

    public long get(E name) {
        return counters.get(name.ordinal());
    }

    public long size() {
        return counters.length();
    }

    public boolean hasCounters() {
        return size() > 0;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        for (E e : enums) {
            map.put(e.name().toLowerCase(Locale.ROOT), counters.get(e.ordinal()));
        }
        return map;
    }

    public static <E extends Enum<E>> EnumCounters<E> merge(Class<E> enumClass, List<EnumCounters<E>> counters) {
        EnumCounters<E> result = new EnumCounters<>(enumClass);
        E[] enums = enumClass.getEnumConstants();
        for (EnumCounters<E> c : counters) {
            for (E e : enums) {
                result.inc(e, c.get(e));
            }
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnumCounters<?> that = (EnumCounters<?>) o;
        return Arrays.equals(toArray(), that.toArray()) && Arrays.equals(enums, that.enums);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(toArray());
        result = 31 * result + Arrays.hashCode(enums);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("[");
        boolean first = true;
        for (E e : enums) {
            buf.append(e.name().toLowerCase(Locale.ROOT)).append(": ").append(get(e));
            if (first) {
                buf.append(", ");
                first = false;
            }
        }
        buf.append("]");
        return buf.toString();
    }

    private long[] toArray() {
        long[] res = new long[enums.length];
        for (int i = 0; i < res.length; i++) {
            res[i] = counters.get(i);
        }
        return res;
    }
}
