/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.support;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The values resolved for the {@link DlsLookup}s of a request, keyed by {@link DlsLookup#key()}.
 * <p>
 * Instances are immutable. The coordinating node resolves the lookups it finds in the request's access control, then writes the
 * result to the {@link AuthorizationServiceField#DLS_LOOKUPS_KEY} request header so it travels with every downstream action.
 * When a shard-level action is authorized again on a data node, the values already present are reused rather than re-resolved.
 * <p>
 * The header is JSON so it is independent of transport versions. Values therefore round-trip with JSON fidelity only, which is
 * sufficient because they are consumed by Mustache rendering.
 */
public final class ResolvedDlsLookups {

    public static final ResolvedDlsLookups EMPTY = new ResolvedDlsLookups(Map.of());

    private final Map<String, Object> valuesByKey;

    public ResolvedDlsLookups(Map<String, Object> valuesByKey) {
        this.valuesByKey = Map.copyOf(Objects.requireNonNull(valuesByKey));
    }

    public boolean isEmpty() {
        return valuesByKey.isEmpty();
    }

    public boolean contains(DlsLookup lookup) {
        return valuesByKey.containsKey(lookup.key());
    }

    /**
     * @return the resolved value for the lookup, or {@code null} if it has not been resolved
     */
    public Object get(DlsLookup lookup) {
        return valuesByKey.get(lookup.key());
    }

    /**
     * Returns a new instance holding these values plus the given ones. Existing keys are not overwritten, since a value that
     * has already been rendered on one shard must not change for another shard of the same request.
     */
    public ResolvedDlsLookups merge(Map<String, Object> additionalValuesByKey) {
        if (additionalValuesByKey.isEmpty()) {
            return this;
        }
        final Map<String, Object> merged = new HashMap<>(valuesByKey);
        additionalValuesByKey.forEach(merged::putIfAbsent);
        return new ResolvedDlsLookups(merged);
    }

    public String encode() {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.map(valuesByKey);
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static ResolvedDlsLookups decode(String encoded) {
        return new ResolvedDlsLookups(XContentHelper.convertToMap(new BytesArray(encoded), false, XContentType.JSON).v2());
    }

    /**
     * Reads the values carried on the thread context, or {@link #EMPTY} if the request has none.
     */
    public static ResolvedDlsLookups readFromContext(ThreadContext threadContext) {
        final String header = threadContext.getHeader(AuthorizationServiceField.DLS_LOOKUPS_KEY);
        return header == null ? EMPTY : decode(header);
    }

    /**
     * Writes the values to the thread context. The header must not already be present; callers that need to replace it must
     * clear it first, for instance via {@link ThreadContext#newStoredContext(java.util.Collection, java.util.Collection)}.
     */
    public void writeToContext(ThreadContext threadContext) {
        threadContext.putHeader(AuthorizationServiceField.DLS_LOOKUPS_KEY, encode());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return valuesByKey.equals(((ResolvedDlsLookups) o).valuesByKey);
    }

    @Override
    public int hashCode() {
        return valuesByKey.hashCode();
    }

    @Override
    public String toString() {
        return "ResolvedDlsLookups" + valuesByKey;
    }
}
