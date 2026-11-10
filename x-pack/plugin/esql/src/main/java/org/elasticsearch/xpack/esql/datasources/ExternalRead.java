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
 * Which read a resolution belongs to: a resource, and the configuration it is read under.
 * <p>
 * Resolution produces a file listing and a schema, and both depend on the configuration as well as on the resource —
 * a CSV listing and a TSV listing of one prefix are different file sets with different schemas. Keyed by the resource
 * alone, two relations reading one resource differently share whichever resolution was computed last, and the other
 * is resolved against settings nobody asked for.
 * <p>
 * <b>Derived, never stored.</b> Nothing carries one of these: {@code EsqlSession} builds them from the relations it
 * walks, and the analyzer rebuilds the same key from the relation it is resolving. A field on a plan node would have
 * to travel to data nodes, and nothing about resolution does — it is finished before a plan is serialized.
 *
 * @param path   the resource, as the relation's {@code tablePath} literal spells it
 * @param config the configuration the relation reads it under, including the data source's credentials, which decide
 *               which objects are visible and so are part of the read rather than incidental to it
 */
public record ExternalRead(String path, Map<String, Object> config) {

    public ExternalRead {
        Objects.requireNonNull(path, "path");
        // Copied rather than aliased, and not with Map.copyOf: a dataset setting may be null, which Map.copyOf
        // refuses, and DefinitionVersion.encodeSettings shows a null value is a shape the config really carries.
        config = config == null ? Map.of() : Collections.unmodifiableMap(new HashMap<>(config));
    }

    /**
     * The read one relation denotes. The one place a relation's two halves become a key, so the session that
     * collects reads and the analyzer that looks one up cannot derive it differently.
     *
     * @throws IllegalStateException if the relation's path is not a non-null literal, which post-parsing it always is
     */
    public static ExternalRead of(UnresolvedExternalRelation relation) {
        Expression tablePath = relation.tablePath();
        if (tablePath instanceof Literal literal && literal.value() != null) {
            return new ExternalRead(BytesRefs.toString(literal.value()), relation.config());
        }
        throw new IllegalStateException(
            "UnresolvedExternalRelation tablePath is not a non-null Literal: ["
                + (tablePath == null ? "null" : tablePath.sourceText())
                + "]"
        );
    }

    /**
     * The path and a digest, never the map. {@code config} carries the data source's credentials, and a collection
     * of these is logged when a resolution is retried and printed by any assertion over a resolution's key set.
     */
    @Override
    public String toString() {
        return path + "#" + Integer.toHexString(config.hashCode());
    }
}
