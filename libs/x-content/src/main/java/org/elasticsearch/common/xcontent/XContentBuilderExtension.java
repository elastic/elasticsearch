/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import java.util.Map;
import java.util.function.Function;

/**
 * This interface provides a way for non-JDK classes to plug in a way to serialize to xcontent.
 *
 * It is <b>greatly</b> preferred that you implement {@link ToXContentFragment}
 * in the class for encoding, however, in some situations you may not own the
 * class, in which case you can add an implementation here for encoding it.
 */
public interface XContentBuilderExtension {

    /**
     * Used for plugging in a generic writer for a class, for example, an example implementation:
     *
     * <pre>
     * {@code
     *     Map<Class<?>, XContentBuilder.Writer> addlWriters = new HashMap<>();
     *     addlWriters.put(BytesRef.class, (builder, value) -> b.value(((BytesRef) value).utf8String()));
     *     return addlWriters;
     * }
     * </pre>
     *
     * @return a map of class name to writer
     */
    Map<Class<?>, XContentBuilder.Writer> getXContentWriters();

    /**
     * Used for plugging in a human readable version of a class's encoding. It is assumed that
     * the human readable equivalent is <b>always</b> behind the {@code toString()} method, so
     * this transformer returns the raw value to be used.
     *
     * An example implementation:
     *
     * <pre>
     * {@code
     *     Map<Class<?>, XContentBuilder.HumanReadableTransformer> transformers = new HashMap<>();
     *     transformers.put(ByteSizeValue.class, (value) -> ((ByteSizeValue) value).bytes());
     * }
     * </pre>
     * @return a map of class name to transformer used to retrieve raw value
     */
    Map<Class<?>, XContentBuilder.HumanReadableTransformer> getXContentHumanReadableTransformers();

    /**
     * Used for plugging a transformer for a date or time type object into a String (or other
     * encodable object).
     *
     * For example:
     *
     * <pre>
     * {@code
     *     final DateTimeFormatter datePrinter = ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC);
     *     Map<Class<?>, Function<Object, Object>> transformers = new HashMap<>();
     *     transformers.put(Date.class, d -> datePrinter.print(((Date) d).getTime()));
     * }
     * </pre>
     */
    Map<Class<?>, Function<Object, Object>> getDateTransformers();
}
