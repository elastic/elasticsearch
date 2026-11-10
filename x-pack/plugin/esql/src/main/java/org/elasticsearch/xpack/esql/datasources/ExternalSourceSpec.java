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
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.util.Collections;
import java.util.HashMap;
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
        // Not Map.copyOf: Dataset.PARSER reads settings with p.map(), so a null value is reachable and copyOf throws.
        config = config == null ? Map.of() : Collections.unmodifiableMap(new HashMap<>(config));
    }

    /**
     * The only derivation point, so the session and the analyzer cannot build different keys for one relation.
     *
     * @throws IllegalStateException if the path is not a non-null literal, which post-parsing it always is
     */
    public static ExternalSourceSpec of(UnresolvedExternalRelation relation) {
        Expression tablePath = relation.tablePath();
        if (tablePath instanceof Literal literal && literal.value() != null) {
            return new ExternalSourceSpec(BytesRefs.toString(literal.value()), relation.config());
        }
        throw new IllegalStateException(
            "UnresolvedExternalRelation tablePath is not a non-null Literal: ["
                + (tablePath == null ? "null" : tablePath.sourceText())
                + "]"
        );
    }

    /** The generated record {@code toString} would print the credentials in {@code config}; one debug log renders these. */
    @Override
    public String toString() {
        return path + "#" + Integer.toHexString(config.hashCode());
    }
}
