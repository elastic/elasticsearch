/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor.model;

import java.util.Map;

/**
 * The per-platform layout variants of a single {@code @StructSpecification} type. Field <em>shape</em>
 * (names, types, getter/setter) is identical across platforms; only the resolved byte offsets and
 * total size may differ, so each supported platform maps to its own fully-resolved
 * {@link StructModel}.
 *
 * <p>Structs with no per-platform layout annotations produce equal models for every platform (they
 * compare {@code equals} as records); code generation groups them so the common case yields a single,
 * non-conditional class initializer.
 *
 * @param simpleName the struct's simple name
 * @param byPlatform resolved model per platform name (keys are {@code Platform} enum constant names)
 */
public record StructVariants(String simpleName, Map<String, StructModel> byPlatform) {

    /**
     * Returns any one platform's model. Safe for shape-only queries (field names, types, record vs
     * interface, nested struct lookup) since those never vary across platforms.
     */
    public StructModel any() {
        return byPlatform.values().iterator().next();
    }
}
