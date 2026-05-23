/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mapper;

import org.elasticsearch.core.Nullable;

import java.util.Map;

/**
 * Utility class for detecting and dispatching BYO (bring-your-own) semantic_text
 * field values. A BYO value is one where the caller supplies pre-computed chunks
 * and vectors rather than asking Elasticsearch to run inference.
 *
 * <p>Two input shapes are recognised:
 * <ul>
 *   <li><em>Multi-part staging</em> — a Map containing an {@code _action} key
 *       (and optionally {@code text}). Each request corresponds to one phase of
 *       a multi-part upload session.</li>
 *   <li><em>Single-shot BYO</em> — a Map containing both {@code text} and
 *       {@code chunks} keys. The full embedding payload is delivered in one
 *       document.</li>
 * </ul>
 *
 * <p>Ordinary inference result Maps (e.g. {@code {"inference": {...}}}) and plain
 * String values are <em>not</em> considered BYO values.
 */
public final class BYOSemanticHandler {

    private BYOSemanticHandler() {}

    /**
     * Returns {@code true} if {@code fieldValue} is a BYO semantic_text input.
     *
     * <p>A value is BYO when it is a {@link Map} that contains either:
     * <ul>
     *   <li>an {@code _action} key (multi-part staging protocol), or</li>
     *   <li>both {@code text} and {@code chunks} keys (single-shot BYO).</li>
     * </ul>
     *
     * @param fieldValue the raw field value from the document; may be {@code null}
     * @return {@code true} for BYO values, {@code false} for strings, nulls, and
     *         non-BYO maps such as pre-computed inference result maps
     */
    public static boolean isBYOValue(@Nullable Object fieldValue) {
        if (fieldValue instanceof Map<?, ?> map) {
            if (map.containsKey(BYOSemanticAction.actionField())) {
                return true;
            }
            return map.containsKey("text") && map.containsKey("chunks");
        }
        return false;
    }

    /**
     * Extracts the {@link BYOSemanticAction} from a BYO field value Map.
     *
     * <p>Returns {@code null} for single-shot BYO values (those that carry
     * {@code text} and {@code chunks} but no {@code _action} key).
     *
     * @param fieldValue a Map that has already been confirmed as a BYO value via
     *                   {@link #isBYOValue(Object)}
     * @return the parsed action, or {@code null} for single-shot BYO
     * @throws IllegalArgumentException if the {@code _action} value is present but
     *                                  unrecognised
     */
    @Nullable
    public static BYOSemanticAction getAction(Map<String, Object> fieldValue) {
        Object actionValue = fieldValue.get(BYOSemanticAction.actionField());
        if (actionValue == null) {
            return null;
        }
        return BYOSemanticAction.fromString(actionValue.toString());
    }
}
