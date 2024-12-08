/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.content;

import com.fasterxml.jackson.core.JsonGenerator;

import org.elasticsearch.xpack.sql.proto.StringUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Utility class providing functionality in XContentBuilder.
 */
public class GeneratorUtils {

    @FunctionalInterface
    private interface Writer {
        void write(JsonGenerator generator, Object value) throws IOException;
    }

    private static Writer valueWriter(Writer writer) {
        return (g, v) -> {
            if (v == null) {
                g.writeNull();
            } else {
                writer.write(g, v);
            }
        };
    }

    private static Writer valuesWriter(Writer writer) {
        return (g, v) -> {
            if (v == null) {
                g.writeNull();
            } else {
                g.writeStartArray();
                writer.write(g, v);
                g.writeEndArray();
            }
        };
    }

    private static final Map<Class<?>, Writer> WRITERS = new HashMap<>();

    static {
        WRITERS.put(Boolean.class, valueWriter((p, v) -> p.writeBoolean((Boolean) v)));
        WRITERS.put(boolean[].class, valuesWriter((p, v) -> {
            for (boolean b : (boolean[]) v) {
                p.writeBoolean(b);
            }
        }));
        WRITERS.put(Byte.class, valueWriter((p, v) -> p.writeNumber((Byte) v)));
        WRITERS.put(byte[].class, valuesWriter((p, v) -> {
            for (byte b : (byte[]) v) {
                p.writeNumber(b);
            }
        }));
        WRITERS.put(Date.class, valueWriter((p, v) -> p.writeNumber(((Date) v).getTime())));
        WRITERS.put(Double.class, valueWriter((p, v) -> p.writeNumber((Double) v)));
        WRITERS.put(double[].class, valuesWriter((p, v) -> {
            for (double d : (double[]) v) {
                p.writeNumber(d);
            }
        }));
        WRITERS.put(Float.class, valueWriter((p, v) -> p.writeNumber((Float) v)));
        WRITERS.put(float[].class, valuesWriter((p, v) -> {
            for (float f : (float[]) v) {
                p.writeNumber(f);
            }
        }));
        WRITERS.put(Integer.class, valueWriter((p, v) -> p.writeNumber((Integer) v)));
        WRITERS.put(int[].class, valuesWriter((p, v) -> {
            for (int i : (int[]) v) {
                p.writeNumber(i);
            }
        }));
        WRITERS.put(Long.class, valueWriter((p, v) -> p.writeNumber((Long) v)));
        WRITERS.put(long[].class, valuesWriter((p, v) -> {
            for (long l : (long[]) v) {
                p.writeNumber(l);
            }
        }));
        WRITERS.put(Short.class, valueWriter((p, v) -> p.writeNumber((Short) v)));
        WRITERS.put(short[].class, valuesWriter((p, v) -> {
            for (short s : (short[]) v) {
                p.writeNumber(s);
            }
        }));
        WRITERS.put(String.class, valueWriter((p, v) -> p.writeString((String) v)));
        WRITERS.put(String[].class, valuesWriter((p, v) -> {
            for (String s : (String[]) v) {
                p.writeString(s);
            }
        }));
        WRITERS.put(Locale.class, valueWriter((p, v) -> p.writeString(v.toString())));
        WRITERS.put(Class.class, valueWriter((p, v) -> p.writeString(v.toString())));
        WRITERS.put(ZonedDateTime.class, valueWriter((p, v) -> p.writeString(StringUtils.toString(v))));
        WRITERS.put(BigInteger.class, valueWriter((p, v) -> p.writeNumber((BigInteger) v)));
        WRITERS.put(BigDecimal.class, valueWriter((p, v) -> p.writeNumber((BigDecimal) v)));
    }

    private GeneratorUtils() {}

    public static void unknownValue(JsonGenerator generator, Object value) throws IOException {
        unknownValue(generator, value, false);
    }

    private static void unknownValue(JsonGenerator generator, Object value, boolean ensureNoSelfReferences) throws IOException {
        if (value == null) {
            generator.writeNull();
            return;
        }
        Writer writer = WRITERS.get(value.getClass());
        if (writer != null) {
            writer.write(generator, value);
        } else if (value instanceof Path) {
            // Path implements Iterable<Path> and causes endless recursion and a StackOverFlow if treated as an Iterable here
            value(generator, (Path) value);
        } else if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            final Map<String, ?> valueMap = (Map<String, ?>) value;
            map(generator, valueMap, ensureNoSelfReferences, true);
        } else if (value instanceof Iterable) {
            iterable(generator, (Iterable<?>) value, ensureNoSelfReferences);
        } else if (value instanceof Object[]) {
            array(generator, (Object[]) value, ensureNoSelfReferences);
        } else if (value instanceof Enum<?>) {
            // Write out the Enum toString
            generator.writeString(Objects.toString(value));
        } else {
            throw new IllegalArgumentException("cannot write xcontent for unknown value of type " + value.getClass());
        }
    }

    private static void map(JsonGenerator generator, Map<String, ?> map, boolean ensureNoSelfReferences, boolean writeStartAndEndHeaders)
        throws IOException {
        if (map == null) {
            generator.writeNull();
            return;
        }

        // checks that the map does not contain references to itself because
        // iterating over map entries will cause a stackoverflow error
        if (ensureNoSelfReferences) {
            ensureNoSelfReferences(map);
        }

        if (writeStartAndEndHeaders) {
            generator.writeStartObject();
        }
        for (Map.Entry<String, ?> value : map.entrySet()) {
            generator.writeFieldName(value.getKey());
            // pass ensureNoSelfReferences=false as we already performed the check at a higher level
            unknownValue(generator, value.getValue(), false);
        }
        if (writeStartAndEndHeaders) {
            generator.writeEndObject();
        }
    }

    private static void iterable(JsonGenerator generator, Iterable<?> values, boolean ensureNoSelfReferences) throws IOException {
        if (values == null) {
            generator.writeNull();
            return;
        }

        if (values instanceof Path) {
            // treat as single value
            value(generator, (Path) values);
        } else {
            // checks that the iterable does not contain references to itself because
            // iterating over entries will cause a stackoverflow error
            if (ensureNoSelfReferences) {
                ensureNoSelfReferences(values);
            }
            generator.writeStartArray();
            for (Object value : values) {
                // pass ensureNoSelfReferences=false as we already performed the check at a higher level
                unknownValue(generator, value, false);
            }
            generator.writeEndArray();
        }
    }

    private static void array(JsonGenerator generator, Object[] values, boolean ensureNoSelfReferences) throws IOException {
        if (values == null) {
            generator.writeNull();
        } else {
            iterable(generator, Arrays.asList(values), ensureNoSelfReferences);
        }
    }

    private static void ensureNoSelfReferences(Object value) {
        Iterable<?> it = convert(value);
        if (it != null) {
            ensureNoSelfReferences(it, value, Collections.newSetFromMap(new IdentityHashMap<>()));
        }
    }

    private static Iterable<?> convert(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map) {
            return ((Map<?, ?>) value).values();
        } else if ((value instanceof Iterable) && (value instanceof Path == false)) {
            return (Iterable<?>) value;
        } else if (value instanceof Object[]) {
            return Arrays.asList((Object[]) value);
        } else {
            return null;
        }
    }

    private static void ensureNoSelfReferences(final Iterable<?> value, Object originalReference, final Set<Object> ancestors) {
        if (value != null) {
            if (ancestors.add(originalReference) == false) {
                throw new IllegalArgumentException("Iterable object is self-referencing itself");
            }
            for (Object o : value) {
                ensureNoSelfReferences(convert(o), o, ancestors);
            }
            ancestors.remove(originalReference);
        }
    }

    public static void value(JsonGenerator generator, Path path) throws IOException {
        if (path == null) {
            generator.writeNull();
        } else {
            generator.writeString(path.toString());
        }
    }
}
