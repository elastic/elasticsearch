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
 * The key a resolution is stored and looked up under. The configuration is part of it: one prefix read as CSV and
 * as TSV yields different columns, so the schema depends on more than the resource.
 * <p>
 * Derived wherever it is needed, never held on a plan node — that would travel to data nodes and need a transport
 * version, and resolution finishes before a plan is serialized.
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
     * Every level, not just the top: {@code DatasetRewriter.mergeSettings} shares its nested maps, so a one-level
     * copy leaves this key's {@code hashCode} free to change while it is a key in the resolution map.
     * Not {@code Map.copyOf}, which throws on the null values {@code Dataset.PARSER} admits.
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
        // Kept by reference, arrays included. Two relations in one query read the same settings instance and a
        // spec never outlives its query, so array identity cannot separate them.
        return value;
    }

    /**
     * The only derivation point, so the session and the analyzer cannot build different keys for one relation.
     *
     * @throws IllegalStateException if the path is not a non-null literal
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
     * As {@link #of}, but {@code null} instead of throwing, for a caller collecting relations into a set: one
     * without a literal {@code tablePath} is simply absent from it.
     */
    static ExternalSourceSpec ofOrNull(UnresolvedExternalRelation relation) {
        Expression tablePath = relation.tablePath();
        return tablePath instanceof Literal literal && literal.value() != null
            ? new ExternalSourceSpec(BytesRefs.toString(literal.value()), relation.config())
            : null;
    }

    /**
     * The resource, and the dataset name when the read came from one. Nothing derived from {@code config}: the
     * generated {@code toString} would print credentials, and a hash of the map is a function of a secret that is
     * plaintext on a cluster which opted out of state encryption. Debug logs render this.
     */
    @Override
    public String toString() {
        Object context = config.get(ExternalSourceResolver.DATASET_CONTEXT_KEY);
        Object name = context instanceof Map<?, ?> map ? map.get("dataset") : null;
        // An https resource may be a pre-signed URL whose query string IS the signature.
        return name == null ? ExternalFailures.redactHttpUrl(path) : ExternalFailures.redactHttpUrl(path) + "[" + name + "]";
    }
}
