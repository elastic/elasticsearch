/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

/**
 * Per-prefix object settings captured during auto-flattening in strict columnar mode.
 * Stored under {@code prefix_properties} in the mapping source, keyed by the full dotted
 * path of the declared object mapper (e.g. {@code "attributes"}, {@code "resource.sub"}).
 *
 * <p>Each facet is nullable — a {@code null} value means the corresponding property was
 * not declared for this prefix. {@link #merge} prefers the incoming non-null value for
 * each facet, preserving the existing value when the incoming one is absent.
 *
 * @param dynamic     the explicit {@link ObjectMapper.Dynamic} setting for this prefix,
 *                    or {@code null} if the prefix does not declare an explicit dynamic value
 * @param passthrough the passthrough priority for this prefix (from a {@link PassThroughObjectMapper}),
 *                    or {@code null} if the prefix is not a passthrough object
 * @param enabled     {@code false} if the object at this prefix is disabled ({@code enabled:false}),
 *                    or {@code null} if the prefix does not declare an explicit enabled value.
 *                    When {@code false}, {@link RootObjectMapper#resolveDynamic} returns
 *                    {@link ObjectMapper.Dynamic#FALSE} for all fields under this prefix,
 *                    and no children are flattened into the mapping at mapping time.
 */
record PrefixProperties(ObjectMapper.Dynamic dynamic, Integer passthrough, Boolean enabled) {

    /**
     * Merges {@code incoming} into {@code existing}, preferring non-null incoming facets.
     * This mirrors the semantics of the old per-map {@code putAll} updates: each facet is
     * independent and the incoming mapping always wins when it supplies a value.
     */
    static PrefixProperties merge(PrefixProperties existing, PrefixProperties incoming) {
        return new PrefixProperties(
            incoming.dynamic != null ? incoming.dynamic : existing.dynamic,
            incoming.passthrough != null ? incoming.passthrough : existing.passthrough,
            incoming.enabled != null ? incoming.enabled : existing.enabled
        );
    }

    /** Returns {@code true} if all facets are {@code null} (this entry carries no information). */
    boolean isEmpty() {
        return dynamic == null && passthrough == null && enabled == null;
    }
}
