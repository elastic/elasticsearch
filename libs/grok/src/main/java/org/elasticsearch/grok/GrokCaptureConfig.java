/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.grok;

import org.joni.NameEntry;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.LongConsumer;

/**
 * Configuration for a value that {@link Grok} can capture.
 */
public final class GrokCaptureConfig {
    private final String name;
    private final GrokCaptureType type;
    private final int[] backRefs;

    GrokCaptureConfig(NameEntry nameEntry) {
        String groupName = new String(nameEntry.name, nameEntry.nameP, nameEntry.nameEnd - nameEntry.nameP, StandardCharsets.UTF_8);
        String[] parts = groupName.split(":");
        name = parts.length >= 2 ? parts[1] : parts[0];
        type = parts.length == 3 ? GrokCaptureType.fromString(parts[2]) : GrokCaptureType.STRING;
        this.backRefs = nameEntry.getBackRefs();
    }

    /**
     * The name defined for the field in the pattern.
     */
    public String name() {
        return name;
    }

    /**
     * The type defined for the field in the pattern.
     */
    GrokCaptureType type() {
        return type;
    }

    /**
     * Build a {@linkplain GrokCaptureExtracter} that will call {@code emit} when
     * it extracts text, boxed if the "native" representation is primitive type.
     * Extracters returned from this method are stateless and can be reused.
     */
    public GrokCaptureExtracter objectExtracter(Consumer<Object> emit) {
        // We could probably write this code a little more concisely but this makes it clear where we are boxing
        return nativeExtracter(new NativeExtracterMap<GrokCaptureExtracter>() {
            @Override
            public GrokCaptureExtracter forString(Function<Consumer<String>, GrokCaptureExtracter> buildExtracter) {
                return buildExtracter.apply(str -> emit.accept(str));
            }

            @Override
            public GrokCaptureExtracter forInt(Function<IntConsumer, GrokCaptureExtracter> buildExtracter) {
                return buildExtracter.apply(i -> emit.accept(Integer.valueOf(i)));
            }

            @Override
            public GrokCaptureExtracter forLong(Function<LongConsumer, GrokCaptureExtracter> buildExtracter) {
                return buildExtracter.apply(l -> emit.accept(Long.valueOf(l)));
            }

            @Override
            public GrokCaptureExtracter forFloat(Function<FloatConsumer, GrokCaptureExtracter> buildExtracter) {
                return buildExtracter.apply(f -> emit.accept(Float.valueOf(f)));
            }

            @Override
            public GrokCaptureExtracter forDouble(Function<DoubleConsumer, GrokCaptureExtracter> buildExtracter) {
                return buildExtracter.apply(d -> emit.accept(Double.valueOf(d)));
            }

            @Override
            public GrokCaptureExtracter forBoolean(Function<Consumer<Boolean>, GrokCaptureExtracter> buildExtracter) {
                return buildExtracter.apply(b -> emit.accept(b));
            }
        });
    }

    /**
     * Build an extract that has access to the "native" type of the extracter
     * match. This means that patterns like {@code %{NUMBER:bytes:float}} has
     * access to an actual {@link float}. Extracters returned from this method
     * should be stateless stateless and can be reused. Pathological implementations
     * of the {@code map} parameter could violate this, but the caller should
     * take care to stay sane.
     * <p>
     * While the goal is to produce a {@link GrokCaptureExtracter} that provides
     * a primitive, the caller can produce whatever type-safe constructs it
     * needs and return them from this method. Thus the {@code <T>} in the type
     * signature.
     *
     * @param <T> The type of the result.
     * @param map Collection of handlers for each native type. Only one method
     *            will be called but well behaved implementers are stateless.
     * @return whatever was returned by the handler.
     */
    public <T> T nativeExtracter(NativeExtracterMap<T> map) {
        return type.nativeExtracter(backRefs, map);
    }

    /**
     * Collection of handlers for each native type. Well behaved implementations
     * are stateless and produce stateless results.
     */
    public interface NativeExtracterMap<T> {
        /**
         * Called when the native type is a {@link String}.
         */
        T forString(Function<Consumer<String>, GrokCaptureExtracter> buildExtracter);

        /**
         * Called when the native type is an {@link int}.
         */
        T forInt(Function<IntConsumer, GrokCaptureExtracter> buildExtracter);

        /**
         * Called when the native type is an {@link long}.
         */
        T forLong(Function<LongConsumer, GrokCaptureExtracter> buildExtracter);

        /**
         * Called when the native type is an {@link float}.
         */
        T forFloat(Function<FloatConsumer, GrokCaptureExtracter> buildExtracter);

        /**
         * Called when the native type is an {@link double}.
         */
        T forDouble(Function<DoubleConsumer, GrokCaptureExtracter> buildExtracter);

        /**
         * Called when the native type is an {@link boolean}.
         */
        T forBoolean(Function<Consumer<Boolean>, GrokCaptureExtracter> buildExtracter);
    }
}
