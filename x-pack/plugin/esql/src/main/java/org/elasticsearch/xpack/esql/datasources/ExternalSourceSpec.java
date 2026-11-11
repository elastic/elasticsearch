/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalFailures;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The key a resolution is stored and looked up under: a resource and the configuration it is read under. Both
 * matter: one prefix read as CSV and as TSV yields different columns, so the schema a resolution carries depends
 * on the configuration and not on the resource alone.
 * <p>
 * Derived, never stored. A field on a plan node would travel to data nodes and need a transport version; resolution
 * finishes before a plan is serialized.
 *
 * @param path   the resource, as the relation's {@code tablePath} literal spells it
 * @param config includes the data source's credentials, which decide which objects are visible
 */
public record ExternalSourceSpec(String path, Map<String, Object> config) {

    public ExternalSourceSpec {
        Objects.requireNonNull(path, "path");
        config = config == null ? Map.of() : deepCopy(config);
    }

    /**
     * Copies every level, not just the top one. {@code DatasetRewriter.mergeSettings} builds the config with
     * {@code putAll}, so its nested maps are shared and mutable; a one-level copy leaves this key's
     * {@code hashCode} free to change underneath it while it is a key in the resolution map, and the analyzer's
     * rebuilt key would then miss.
     * <p>
     * Not {@code Map.copyOf}: {@code Dataset.PARSER} reads settings with {@code p.map()}, so a null value is
     * reachable and {@code copyOf} throws on it.
     */
    private static Map<String, Object> deepCopy(Map<String, Object> config) {
        Map<String, Object> copy = new HashMap<>(config.size());
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            copy.put(entry.getKey(), deepCopyValue(entry.getValue()));
        }
        return Collections.unmodifiableMap(copy);
    }

    @SuppressWarnings("unchecked")
    private static Object deepCopyValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            return deepCopy((Map<String, Object>) map);
        }
        if (value instanceof List<?> list) {
            List<Object> copy = new ArrayList<>(list.size());
            list.forEach(element -> copy.add(deepCopyValue(element)));
            return Collections.unmodifiableList(copy);
        }
        return value;
    }

    /**
     * The only derivation point, so the session and the analyzer cannot build different keys for one relation.
     *
     * @throws IllegalStateException if the path is not a non-null literal, which post-parsing it always is
     */
    public static ExternalSourceSpec of(UnresolvedExternalRelation relation) {
        ExternalSourceSpec spec = ofOrNull(relation);
        if (spec != null) {
            return spec;
        }
        Expression tablePath = relation.tablePath();
        throw new IllegalStateException(
            "UnresolvedExternalRelation tablePath is not a non-null Literal: ["
                + (tablePath == null ? "null" : tablePath.sourceText())
                + "]"
        );
    }

    /**
     * As {@link #of}, but {@code null} for a relation whose {@code tablePath} is not a non-null literal — a shape
     * pre-analysis can still see. A caller that must not throw there gets the same derivation and no exception, and
     * the relation is simply absent from the set it was being collected into.
     */
    public static ExternalSourceSpec ofOrNull(UnresolvedExternalRelation relation) {
        Expression tablePath = relation.tablePath();
        return tablePath instanceof Literal literal && literal.value() != null
            ? new ExternalSourceSpec(BytesRefs.toString(literal.value()), relation.config())
            : null;
    }

    /**
     * The resource, and the dataset name when the read came from one. Nothing derived from {@code config}: the
     * generated record {@code toString} would print the credentials outright, and a hash of the map is a function of
     * a secret that is plaintext on a cluster which opted out of state encryption. One debug log renders these.
     */
    @Override
    public String toString() {
        Object context = config.get(ExternalSourceResolver.DATASET_CONTEXT_KEY);
        Object name = context instanceof Map<?, ?> map ? map.get("dataset") : null;
        // Redacted for the same reason RequestFilterRewriter redacts a relation's path: an https resource may be a
        // pre-signed URL whose query string IS the signature.
        return name == null ? ExternalFailures.redactHttpUrl(path) : ExternalFailures.redactHttpUrl(path) + "[" + name + "]";
    }
}
