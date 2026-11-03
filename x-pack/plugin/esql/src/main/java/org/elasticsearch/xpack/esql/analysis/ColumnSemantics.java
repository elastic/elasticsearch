/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.FieldSemantics;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Links the columns of a plan back to the {@link FieldSemantics} of the mapped fields they read.
 * <p>
 * A column keeps its semantics when it passes through unchanged or is renamed. Computed columns
 * have none.
 */
public final class ColumnSemantics {

    private ColumnSemantics() {}

    /**
     * Returns the semantics of every attribute in {@code plan} that reads a mapped field, keyed by attribute id.
     */
    public static Map<NameId, FieldSemantics> collect(LogicalPlan plan, Collection<IndexResolution> resolutions) {
        Map<String, EsIndex> indices = new HashMap<>();
        for (IndexResolution resolution : resolutions) {
            if (resolution != null && resolution.isValid()) {
                indices.put(resolution.get().name(), resolution.get());
            }
        }
        Map<NameId, FieldSemantics> semantics = new HashMap<>();
        if (indices.isEmpty()) {
            return semantics;
        }
        plan.forEachUp(EsRelation.class, relation -> {
            EsIndex index = indices.get(relation.indexPattern());
            if (index == null) {
                return;
            }
            for (Attribute attribute : relation.output()) {
                if (attribute instanceof FieldAttribute field) {
                    FieldSemantics fieldSemantics = index.fieldSemantics().get(field.fieldName().string());
                    if (fieldSemantics != null) {
                        semantics.put(field.id(), fieldSemantics);
                    }
                }
            }
        });
        // RENAME and pass-through aliases keep the semantics of the field they point at.
        plan.forEachExpressionUp(Alias.class, alias -> {
            if (alias.child() instanceof Attribute source) {
                FieldSemantics fieldSemantics = semantics.get(source.id());
                if (fieldSemantics != null) {
                    semantics.put(alias.id(), fieldSemantics);
                }
            }
        });
        return semantics;
    }
}
