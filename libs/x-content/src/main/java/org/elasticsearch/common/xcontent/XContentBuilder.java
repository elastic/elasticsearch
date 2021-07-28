/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.RestApiVersion;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.FilterOutputStream;
import java.io.Flushable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Function;

/**
 * A utility to build XContent (ie json).
 */
public final class XContentBuilder implements Closeable, Flushable {

    /**
     * Create a new {@link XContentBuilder} using the given {@link XContent} content.
     * <p>
     * The builder uses an internal {@link ByteArrayOutputStream} output stream to build the content.
     * </p>
     *
     * @param xContent the {@link XContent}
     * @return a new {@link XContentBuilder}
     * @throws IOException if an {@link IOException} occurs while building the content
     */
    public static XContentBuilder builder(XContent xContent) throws IOException {
        return new XContentBuilder(xContent, new ByteArrayOutputStream());
    }

    /**
     * Create a new {@link XContentBuilder} using the given {@link XContent} content and RestApiVersion.
     * <p>
     * The builder uses an internal {@link ByteArrayOutputStream} output stream to build the content.
     * </p>
     *
     * @param xContent the {@link XContent}
     * @return a new {@link XContentBuilder}
     * @throws IOException if an {@link IOException} occurs while building the content
     */
    public static XContentBuilder builder(XContent xContent, RestApiVersion restApiVersion) throws IOException {
        return new XContentBuilder(xContent, new ByteArrayOutputStream(),
            Collections.emptySet(),
            Collections.emptySet(),
            xContent.type().toParsedMediaType(),
            restApiVersion);
    }

    /**
     * Create a new {@link XContentBuilder} using the given {@link XContentType} xContentType and some inclusive and/or exclusive filters.
     * <p>
     * The builder uses an internal {@link ByteArrayOutputStream} output stream to build the content. When both exclusive and
     * inclusive filters are provided, the underlying builder will first use exclusion filters to remove fields and then will check the
     * remaining fields against the inclusive filters.
     * <p>
     *
     * @param xContentType the {@link XContentType}
     * @param includes the inclusive filters: only fields and objects that match the inclusive filters will be written to the output.
     * @param excludes the exclusive filters: only fields and objects that don't match the exclusive filters will be written to the output.
     * @throws IOException if an {@link IOException} occurs while building the content
     */
    public static XContentBuilder builder(XContentType xContentType, Set<String> includes, Set<String> excludes) throws IOException {
        return new XContentBuilder(xContentType.xContent(), new ByteArrayOutputStream(), includes, excludes,
            xContentType.toParsedMediaType(), RestApiVersion.current());
    }

    private static final Map<Class<?>, Writer> WRITERS;
    private static final Map<Class<?>, HumanReadableTransformer> HUMAN_READABLE_TRANSFORMERS;
    private static final Map<Class<?>, Function<Object, Object>> DATE_TRANSFORMERS;
    static {
        Map<Class<?>, Writer> writers = new HashMap<>();
        writers.put(Boolean.class, (b, v) -> b.value((Boolean) v));
        writers.put(boolean[].class, (b, v) -> b.values((boolean[]) v));
        writers.put(Byte.class, (b, v) -> b.value((Byte) v));
        writers.put(byte[].class, (b, v) -> b.value((byte[]) v));
        writers.put(Date.class, XContentBuilder::timeValue);
        writers.put(Double.class, (b, v) -> b.value((Double) v));
        writers.put(double[].class, (b, v) -> b.values((double[]) v));
        writers.put(Float.class, (b, v) -> b.value((Float) v));
        writers.put(float[].class, (b, v) -> b.values((float[]) v));
        writers.put(Integer.class, (b, v) -> b.value((Integer) v));
        writers.put(int[].class, (b, v) -> b.values((int[]) v));
        writers.put(Long.class, (b, v) -> b.value((Long) v));
        writers.put(long[].class, (b, v) -> b.values((long[]) v));
        writers.put(Short.class, (b, v) -> b.value((Short) v));
        writers.put(short[].class, (b, v) -> b.values((short[]) v));
        writers.put(String.class, (b, v) -> b.value((String) v));
        writers.put(String[].class, (b, v) -> b.values((String[]) v));
        writers.put(Locale.class, (b, v) -> b.value(v.toString()));
        writers.put(Class.class, (b, v) -> b.value(v.toString()));
        writers.put(ZonedDateTime.class, (b, v) -> b.value(v.toString()));
        writers.put(Calendar.class, XContentBuilder::timeValue);
        writers.put(GregorianCalendar.class, XContentBuilder::timeValue);
        writers.put(BigInteger.class, (b, v) -> b.value((BigInteger) v));
        writers.put(BigDecimal.class, (b, v) -> b.value((BigDecimal) v));

        Map<Class<?>, HumanReadableTransformer> humanReadableTransformer = new HashMap<>();
        Map<Class<?>, Function<Object, Object>> dateTransformers = new HashMap<>();

        // treat strings as already converted
        dateTransformers.put(String.class, Function.identity());

        // Load pluggable extensions
        for (XContentBuilderExtension service : ServiceLoader.load(XContentBuilderExtension.class)) {
            Map<Class<?>, Writer> addlWriters = service.getXContentWriters();
            Map<Class<?>, HumanReadableTransformer> addlTransformers = service.getXContentHumanReadableTransformers();
            Map<Class<?>, Function<Object, Object>> addlDateTransformers = service.getDateTransformers();

            addlWriters.forEach((key, value) -> Objects.requireNonNull(value,
                "invalid null xcontent writer for class " + key));
            addlTransformers.forEach((key, value) -> Objects.requireNonNull(value,
                "invalid null xcontent transformer for human readable class " + key));
            dateTransformers.forEach((key, value) -> Objects.requireNonNull(value,
                "invalid null xcontent date transformer for class " + key));

            writers.putAll(addlWriters);
            humanReadableTransformer.putAll(addlTransformers);
            dateTransformers.putAll(addlDateTransformers);
        }

        WRITERS = Collections.unmodifiableMap(writers);
        HUMAN_READABLE_TRANSFORMERS = Collections.unmodifiableMap(humanReadableTransformer);
        DATE_TRANSFORMERS = Collections.unmodifiableMap(dateTransformers);
    }

    @FunctionalInterface
    public interface Writer {
        void write(XContentBuilder builder, Object value) throws IOException;
    }

    /**
     * Interface for transforming complex objects into their "raw" equivalents for human-readable fields
     */
    @FunctionalInterface
    public interface HumanReadableTransformer {
        Object rawValue(Object value) throws IOException;
    }

    /**
     * XContentGenerator used to build the XContent object
     */
    private final XContentGenerator generator;

    /**
     * Output stream to which the built object is written
     */
    private final OutputStream bos;

    private final RestApiVersion restApiVersion;

    private final ParsedMediaType responseContentType;

    /**
     * When this flag is set to true, some types of values are written in a format easier to read for a human.
     */
    private boolean humanReadable = false;



    /**
     * Constructs a new builder using the provided XContent and an OutputStream. Make sure
     * to call {@link #close()} when the builder is done with.
     */
    public XContentBuilder(XContent xContent, OutputStream bos) throws IOException {
        this(xContent, bos, Collections.emptySet(), Collections.emptySet(), xContent.type().toParsedMediaType(), RestApiVersion.current());
    }
    /**
     * Constructs a new builder using the provided XContent, an OutputStream and
     * some filters. If filters are specified, only those values matching a
     * filter will be written to the output stream. Make sure to call
     * {@link #close()} when the builder is done with.
     */
    public XContentBuilder(XContentType xContentType, OutputStream bos, Set<String> includes) throws IOException {
        this(xContentType.xContent(), bos, includes, Collections.emptySet(), xContentType.toParsedMediaType(), RestApiVersion.current());
    }

    /**
     * Creates a new builder using the provided XContent, output stream and some inclusive and/or exclusive filters. When both exclusive and
     * inclusive filters are provided, the underlying builder will first use exclusion filters to remove fields and then will check the
     * remaining fields against the inclusive filters.
     * <p>
     * Make sure to call {@link #close()} when the builder is done with.
     * @param os       the output stream
     * @param includes the inclusive filters: only fields and objects that match the inclusive filters will be written to the output.
     * @param excludes the exclusive filters: only fields and objects that don't match the exclusive filters will be written to the output.
     * @param responseContentType  a content-type header value to be send back on a response
     */
    public XContentBuilder(XContent xContent, OutputStream os, Set<String> includes, Set<String> excludes,
                           ParsedMediaType responseContentType) throws IOException {
        this(xContent, os, includes, excludes, responseContentType, RestApiVersion.current());
    }

    /**
     * Creates a new builder using the provided XContent, output stream and some inclusive and/or exclusive filters. When both exclusive and
     * inclusive filters are provided, the underlying builder will first use exclusion filters to remove fields and then will check the
     * remaining fields against the inclusive filters.
     * Stores RestApiVersion to help steer the use of the builder depending on the version.
     * @see #getRestApiVersion()
     * <p>
     * Make sure to call {@link #close()} when the builder is done with.
     * @param os       the output stream
     * @param includes the inclusive filters: only fields and objects that match the inclusive filters will be written to the output.
     * @param excludes the exclusive filters: only fields and objects that don't match the exclusive filters will be written to the output.
     * @param responseContentType  a content-type header value to be send back on a response
     * @param restApiVersion a rest api version indicating with which version the XContent is compatible with.
     */
    public XContentBuilder(XContent xContent, OutputStream os, Set<String> includes, Set<String> excludes,
                           ParsedMediaType responseContentType, RestApiVersion restApiVersion) throws IOException {
        this.bos = os;
        assert responseContentType != null : "generated response cannot be null";
        this.responseContentType = responseContentType;
        this.generator = xContent.createGenerator(bos, includes, excludes);
        this.restApiVersion = restApiVersion;
    }

    public String getResponseContentTypeString() {
        return responseContentType.responseContentTypeHeader();
    }

    public XContentType contentType() {
        return generator.contentType();
    }

    /**
     * @return the output stream to which the built object is being written. Note that is dangerous to modify the stream.
     */
    public OutputStream getOutputStream() {
        return bos;
    }

    public XContentBuilder prettyPrint() {
        generator.usePrettyPrint();
        return this;
    }

    public boolean isPrettyPrint() {
        return generator.isPrettyPrint();
    }

    /**
     * Indicate that the current {@link XContentBuilder} must write a line feed ("\n")
     * at the end of the built object.
     * <p>
     * This only applies for JSON XContent type. It has no effect for other types.
     */
    public XContentBuilder lfAtEnd() {
        generator.usePrintLineFeedAtEnd();
        return this;
    }

    /**
     * Set the "human readable" flag. Once set, some types of values are written in a
     * format easier to read for a human.
     */
    public XContentBuilder humanReadable(boolean humanReadable) {
        this.humanReadable = humanReadable;
        return this;
    }

    /**
     * @return the value of the "human readable" flag. When the value is equal to true,
     * some types of values are written in a format easier to read for a human.
     */
    public boolean humanReadable() {
        return this.humanReadable;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Structure (object, array, field, null values...)
    //////////////////////////////////

    public XContentBuilder startObject() throws IOException {
        generator.writeStartObject();
        return this;
    }

    public XContentBuilder startObject(String name) throws IOException {
        return field(name).startObject();
    }

    public XContentBuilder endObject() throws IOException {
        generator.writeEndObject();
        return this;
    }

    public XContentBuilder startArray() throws IOException {
        generator.writeStartArray();
        return this;
    }

    public XContentBuilder startArray(String name) throws IOException {
        return field(name).startArray();
    }

    public XContentBuilder endArray() throws IOException {
        generator.writeEndArray();
        return this;
    }

    public XContentBuilder field(String name) throws IOException {
        ensureNameNotNull(name);
        generator.writeFieldName(name);
        return this;
    }

    public XContentBuilder nullField(String name) throws IOException {
        ensureNameNotNull(name);
        generator.writeNullField(name);
        return this;
    }

    public XContentBuilder nullValue() throws IOException {
        generator.writeNull();
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Boolean
    //////////////////////////////////

    public XContentBuilder field(String name, Boolean value) throws IOException {
        return (value == null) ? nullField(name) : field(name, value.booleanValue());
    }

    public XContentBuilder field(String name, boolean value) throws IOException {
        ensureNameNotNull(name);
        generator.writeBooleanField(name, value);
        return this;
    }

    public XContentBuilder array(String name, boolean[] values) throws IOException {
        return field(name).values(values);
    }

    private XContentBuilder values(boolean[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (boolean b : values) {
            value(b);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(Boolean value) throws IOException {
        return (value == null) ? nullValue() : value(value.booleanValue());
    }

    public XContentBuilder value(boolean value) throws IOException {
        generator.writeBoolean(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Byte
    //////////////////////////////////

    public XContentBuilder field(String name, Byte value) throws IOException {
        return (value == null) ? nullField(name) : field(name, value.byteValue());
    }

    public XContentBuilder field(String name, byte value) throws IOException {
        return field(name).value(value);
    }

    public XContentBuilder value(Byte value) throws IOException {
        return (value == null) ? nullValue() : value(value.byteValue());
    }

    public XContentBuilder value(byte value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Double
    //////////////////////////////////

    public XContentBuilder field(String name, Double value) throws IOException {
        return (value == null) ? nullField(name) : field(name, value.doubleValue());
    }

    public XContentBuilder field(String name, double value) throws IOException {
        ensureNameNotNull(name);
        generator.writeNumberField(name, value);
        return this;
    }

    public XContentBuilder array(String name, double[] values) throws IOException {
        return field(name).values(values);
    }

    private XContentBuilder values(double[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (double b : values) {
            value(b);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(Double value) throws IOException {
        return (value == null) ? nullValue() : value(value.doubleValue());
    }

    public XContentBuilder value(double value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Float
    //////////////////////////////////

    public XContentBuilder field(String name, Float value) throws IOException {
        return (value == null) ? nullField(name) : field(name, value.floatValue());
    }

    public XContentBuilder field(String name, float value) throws IOException {
        ensureNameNotNull(name);
        generator.writeNumberField(name, value);
        return this;
    }

    public XContentBuilder array(String name, float[] values) throws IOException {
        return field(name).values(values);
    }

    private XContentBuilder values(float[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (float f : values) {
            value(f);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(Float value) throws IOException {
        return (value == null) ? nullValue() : value(value.floatValue());
    }

    public XContentBuilder value(float value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Integer
    //////////////////////////////////

    public XContentBuilder field(String name, Integer value) throws IOException {
        return (value == null) ? nullField(name) : field(name, value.intValue());
    }

    public XContentBuilder field(String name, int value) throws IOException {
        ensureNameNotNull(name);
        generator.writeNumberField(name, value);
        return this;
    }

    public XContentBuilder array(String name, int[] values) throws IOException {
        return field(name).values(values);
    }

    private XContentBuilder values(int[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (int i : values) {
            value(i);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(Integer value) throws IOException {
        return (value == null) ? nullValue() : value(value.intValue());
    }

    public XContentBuilder value(int value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Long
    //////////////////////////////////

    public XContentBuilder field(String name, Long value) throws IOException {
        return (value == null) ? nullField(name) : field(name, value.longValue());
    }

    public XContentBuilder field(String name, long value) throws IOException {
        ensureNameNotNull(name);
        generator.writeNumberField(name, value);
        return this;
    }

    public XContentBuilder array(String name, long[] values) throws IOException {
        return field(name).values(values);
    }

    private XContentBuilder values(long[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (long l : values) {
            value(l);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(Long value) throws IOException {
        return (value == null) ? nullValue() : value(value.longValue());
    }

    public XContentBuilder value(long value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Short
    //////////////////////////////////

    public XContentBuilder field(String name, Short value) throws IOException {
        return (value == null) ? nullField(name) : field(name, value.shortValue());
    }

    public XContentBuilder field(String name, short value) throws IOException {
        return field(name).value(value);
    }

    public XContentBuilder array(String name, short[] values) throws IOException {
        return field(name).values(values);
    }

    private XContentBuilder values(short[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (short s : values) {
            value(s);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(Short value) throws IOException {
        return (value == null) ? nullValue() : value(value.shortValue());
    }

    public XContentBuilder value(short value) throws IOException {
        generator.writeNumber(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // BigInteger
    //////////////////////////////////

    public XContentBuilder field(String name, BigInteger value) throws IOException {
        if (value == null) {
            return nullField(name);
        }
        ensureNameNotNull(name);
        generator.writeNumberField(name, value);
        return this;
    }

    public XContentBuilder array(String name, BigInteger[] values) throws IOException {
        return field(name).values(values);
    }

    private XContentBuilder values(BigInteger[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (BigInteger b : values) {
            value(b);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(BigInteger value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generator.writeNumber(value);
        return this;
    }


    ////////////////////////////////////////////////////////////////////////////
    // BigDecimal
    //////////////////////////////////

    public XContentBuilder field(String name, BigDecimal value) throws IOException {
        if (value == null) {
            return nullField(name);
        }
        ensureNameNotNull(name);
        generator.writeNumberField(name, value);
        return this;
    }

    public XContentBuilder array(String name, BigDecimal[] values) throws IOException {
        return field(name).values(values);
    }

    private XContentBuilder values(BigDecimal[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (BigDecimal b : values) {
            value(b);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(BigDecimal value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generator.writeNumber(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // String
    //////////////////////////////////

    public XContentBuilder field(String name, String value) throws IOException {
        if (value == null) {
            return nullField(name);
        }
        ensureNameNotNull(name);
        generator.writeStringField(name, value);
        return this;
    }

    public XContentBuilder array(String name, String... values) throws IOException {
        return field(name).values(values);
    }

    private XContentBuilder values(String[] values) throws IOException {
        if (values == null) {
            return nullValue();
        }
        startArray();
        for (String s : values) {
            value(s);
        }
        endArray();
        return this;
    }

    public XContentBuilder value(String value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generator.writeString(value);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Binary
    //////////////////////////////////

    public XContentBuilder field(String name, byte[] value) throws IOException {
        if (value == null) {
            return nullField(name);
        }
        ensureNameNotNull(name);
        generator.writeBinaryField(name, value);
        return this;
    }

    public XContentBuilder value(byte[] value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generator.writeBinary(value);
        return this;
    }

    public XContentBuilder field(String name, byte[] value, int offset, int length) throws IOException {
        return field(name).value(value, offset, length);
    }

    public XContentBuilder value(byte[] value, int offset, int length) throws IOException {
        if (value == null) {
            return nullValue();
        }
        generator.writeBinary(value, offset, length);
        return this;
    }

    /**
     * Writes the binary content of the given byte array as UTF-8 bytes.
     *
     * Use {@link XContentParser#charBuffer()} to read the value back
     */
    public XContentBuilder utf8Value(byte[] bytes, int offset, int length) throws IOException {
        generator.writeUTF8String(bytes, offset, length);
        return this;
    }


    ////////////////////////////////////////////////////////////////////////////
    // Date
    //////////////////////////////////

    /**
     * Write a time-based field and value, if the passed timeValue is null a
     * null value is written, otherwise a date transformers lookup is performed.

     * @throws IllegalArgumentException if there is no transformers for the type of object
     */
    public XContentBuilder timeField(String name, Object timeValue) throws IOException {
        return field(name).timeValue(timeValue);
    }

    /**
     * If the {@code humanReadable} flag is set, writes both a formatted and
     * unformatted version of the time value using the date transformer for the
     * {@link Long} class.
     */
    public XContentBuilder timeField(String name, String readableName, long value) throws IOException {
        assert name.equals(readableName) == false :
            "expected raw and readable field names to differ, but they were both: " + name;
        if (humanReadable) {
            Function<Object, Object> longTransformer = DATE_TRANSFORMERS.get(Long.class);
            if (longTransformer == null) {
                throw new IllegalArgumentException("cannot write time value xcontent for unknown value of type Long");
            }
            field(readableName).value(longTransformer.apply(value));
        }
        field(name, value);
        return this;
    }

    /**
     * Write a time-based value, if the value is null a null value is written,
     * otherwise a date transformers lookup is performed.

     * @throws IllegalArgumentException if there is no transformers for the type of object
     */
    public XContentBuilder timeValue(Object timeValue) throws IOException {
        if (timeValue == null) {
            return nullValue();
        } else {
            Function<Object, Object> transformer = DATE_TRANSFORMERS.get(timeValue.getClass());
            if (transformer == null) {
                throw new IllegalArgumentException("cannot write time value xcontent for unknown value of type " + timeValue.getClass());
            }
            return value(transformer.apply(timeValue));
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // LatLon
    //////////////////////////////////

    public XContentBuilder latlon(String name, double lat, double lon) throws IOException {
        return field(name).latlon(lat, lon);
    }

    public XContentBuilder latlon(double lat, double lon) throws IOException {
        return startObject().field("lat", lat).field("lon", lon).endObject();
    }

    ////////////////////////////////////////////////////////////////////////////
    // Path
    //////////////////////////////////

    public XContentBuilder value(Path value) throws IOException {
        if (value == null) {
            return nullValue();
        }
        return value(value.toString());
    }

    ////////////////////////////////////////////////////////////////////////////
    // Objects
    //
    // These methods are used when the type of value is unknown. It tries to fallback
    // on typed methods and use Object.toString() as a last resort. Always prefer using
    // typed methods over this.
    //////////////////////////////////

    public XContentBuilder field(String name, Object value) throws IOException {
        return field(name).value(value);
    }

    public XContentBuilder array(String name, Object... values) throws IOException {
        return field(name).values(values, true);
    }

    private XContentBuilder values(Object[] values, boolean ensureNoSelfReferences) throws IOException {
        if (values == null) {
            return nullValue();
        }
        return value(Arrays.asList(values), ensureNoSelfReferences);
    }

    public XContentBuilder value(Object value) throws IOException {
        unknownValue(value, true);
        return this;
    }

    private void unknownValue(Object value, boolean ensureNoSelfReferences) throws IOException {
        if (value == null) {
            nullValue();
            return;
        }
        Writer writer = WRITERS.get(value.getClass());
        if (writer != null) {
            writer.write(this, value);
        } else if (value instanceof Path) {
            //Path implements Iterable<Path> and causes endless recursion and a StackOverFlow if treated as an Iterable here
            value((Path) value);
        } else if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            final Map<String, ?> valueMap = (Map<String, ?>) value;
            map(valueMap, ensureNoSelfReferences, true);
        } else if (value instanceof Iterable) {
            value((Iterable<?>) value, ensureNoSelfReferences);
        } else if (value instanceof Object[]) {
            values((Object[]) value, ensureNoSelfReferences);
        } else if (value instanceof ToXContent) {
            value((ToXContent) value);
        } else if (value instanceof Enum<?>) {
            // Write out the Enum toString
            value(Objects.toString(value));
        } else {
            throw new IllegalArgumentException("cannot write xcontent for unknown value of type " + value.getClass());
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // ToXContent
    //////////////////////////////////

    public XContentBuilder field(String name, ToXContent value) throws IOException {
        return field(name).value(value);
    }

    public XContentBuilder field(String name, ToXContent value, ToXContent.Params params) throws IOException {
        return field(name).value(value, params);
    }

    private XContentBuilder value(ToXContent value) throws IOException {
        return value(value, ToXContent.EMPTY_PARAMS);
    }

    private XContentBuilder value(ToXContent value, ToXContent.Params params) throws IOException {
        if (value == null) {
            return nullValue();
        }
        value.toXContent(this, params);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Maps & Iterable
    //////////////////////////////////

    public XContentBuilder field(String name, Map<String, Object> values) throws IOException {
        return field(name).map(values);
    }

    public XContentBuilder map(Map<String, ?> values) throws IOException {
        return map(values, true, true);
    }

    /** writes a map without the start object and end object headers */
    public XContentBuilder mapContents(Map<String, ?> values) throws IOException {
        return map(values, true, false);
    }

    private XContentBuilder map(Map<String, ?> values, boolean ensureNoSelfReferences, boolean writeStartAndEndHeaders) throws IOException {
        if (values == null) {
            return nullValue();
        }

        // checks that the map does not contain references to itself because
        // iterating over map entries will cause a stackoverflow error
        if (ensureNoSelfReferences) {
            ensureNoSelfReferences(values);
        }

        if (writeStartAndEndHeaders) {
            startObject();
        }
        for (Map.Entry<String, ?> value : values.entrySet()) {
            field(value.getKey());
            // pass ensureNoSelfReferences=false as we already performed the check at a higher level
            unknownValue(value.getValue(), false);
        }
        if (writeStartAndEndHeaders) {
            endObject();
        }
        return this;
    }

    public XContentBuilder field(String name, Iterable<?> values) throws IOException {
        return field(name).value(values);
    }

    private XContentBuilder value(Iterable<?> values, boolean ensureNoSelfReferences) throws IOException {
        if (values == null) {
            return nullValue();
        }

        if (values instanceof Path) {
            //treat as single value
            value((Path) values);
        } else {
            // checks that the iterable does not contain references to itself because
            // iterating over entries will cause a stackoverflow error
            if (ensureNoSelfReferences) {
                ensureNoSelfReferences(values);
            }
            startArray();
            for (Object value : values) {
                // pass ensureNoSelfReferences=false as we already performed the check at a higher level
                unknownValue(value, false);
            }
            endArray();
        }
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Human readable fields
    //
    // These are fields that have a "raw" value and a "human readable" value,
    // such as time values or byte sizes. The human readable variant is only
    // used if the humanReadable flag has been set
    //////////////////////////////////

    public XContentBuilder humanReadableField(String rawFieldName, String readableFieldName, Object value) throws IOException {
        assert rawFieldName.equals(readableFieldName) == false :
            "expected raw and readable field names to differ, but they were both: " + rawFieldName;
        if (humanReadable) {
            field(readableFieldName, Objects.toString(value));
        }
        HumanReadableTransformer transformer = HUMAN_READABLE_TRANSFORMERS.get(value.getClass());
        if (transformer != null) {
            Object rawValue = transformer.rawValue(value);
            field(rawFieldName, rawValue);
        } else {
            throw new IllegalArgumentException("no raw transformer found for class " + value.getClass());
        }
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Misc.
    //////////////////////////////////


    public XContentBuilder percentageField(String rawFieldName, String readableFieldName, double percentage) throws IOException {
        assert rawFieldName.equals(readableFieldName) == false :
            "expected raw and readable field names to differ, but they were both: " + rawFieldName;
        if (humanReadable) {
            field(readableFieldName, String.format(Locale.ROOT, "%1.1f%%", percentage));
        }
        field(rawFieldName, percentage);
        return this;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Raw fields
    //////////////////////////////////

    /**
     * Writes a raw field with the value taken from the bytes in the stream
     * @deprecated use {@link #rawField(String, InputStream, XContentType)} to avoid content type auto-detection
     */
    @Deprecated
    public XContentBuilder rawField(String name, InputStream value) throws IOException {
        generator.writeRawField(name, value);
        return this;
    }

    /**
     * Writes a raw field with the value taken from the bytes in the stream
     */
    public XContentBuilder rawField(String name, InputStream value, XContentType contentType) throws IOException {
        generator.writeRawField(name, value, contentType);
        return this;
    }

    /**
     * Writes a value with the source coming directly from the bytes in the stream
     */
    public XContentBuilder rawValue(InputStream stream, XContentType contentType) throws IOException {
        generator.writeRawValue(stream, contentType);
        return this;
    }

    public XContentBuilder copyCurrentStructure(XContentParser parser) throws IOException {
        generator.copyCurrentStructure(parser);
        return this;
    }

    /**
     * Write the content that is written to the output stream by the {@code writer} as a string encoded in Base64 format.
     * This API can be used to generate XContent directly without the intermediate results to reduce memory usage.
     * Note that this method supports only JSON.
     */
    public XContentBuilder directFieldAsBase64(String name, CheckedConsumer<OutputStream, IOException> writer) throws IOException {
        if (contentType() != XContentType.JSON) {
            assert false : "directFieldAsBase64 supports only JSON format";
            throw new UnsupportedOperationException("directFieldAsBase64 supports only JSON format");
        }
        generator.writeDirectField(name, os -> {
            os.write('\"');
            final FilterOutputStream noClose = new FilterOutputStream(os) {
                @Override
                public void close() {
                    // We need to close the output stream that is wrapped by a Base64 encoder to flush the outstanding buffer
                    // of the encoder, but we must not close the underlying output stream of the XContentBuilder.
                }
            };
            final OutputStream encodedOutput = Base64.getEncoder().wrap(noClose);
            writer.accept(encodedOutput);
            encodedOutput.close(); // close to flush the outstanding buffer used in the Base64 Encoder
            os.write('\"');
        });
        return this;
    }

    /**
     * Returns a version used for serialising a response.
     * @return a compatible version
     */
    public RestApiVersion getRestApiVersion() {
        return restApiVersion;
    }

    @Override
    public void flush() throws IOException {
        generator.flush();
    }

    @Override
    public void close() {
        try {
            generator.close();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to close the XContentBuilder", e);
        }
    }

    public XContentGenerator generator() {
        return this.generator;
    }

    static void ensureNameNotNull(String name) {
        ensureNotNull(name, "Field name cannot be null");
    }

    static void ensureNotNull(Object value, String message) {
        if (value == null) {
            throw new IllegalArgumentException(message);
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
            return ((Map<?,?>) value).values();
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

}
