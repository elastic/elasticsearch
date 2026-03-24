/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * Verifies {@code unmapped_fields="load"} restrictions for partially mapped fields that are not KEYWORD
 * (see #141995, #141994, #144228 for context).
 */
final class LoadPartialMappingChecks {

    private LoadPartialMappingChecks() {}

    /**
     * When {@code unmapped_fields="load"} is active, fail if the query <strong>uses</strong> (via pipeline
     * commands / expressions outside of {@link EsRelation}) a field that is partially unmapped on any
     * non-lookup index in the plan and is not KEYWORD where mapped. Lookup relations are skipped: load does
     * not apply to LOOKUP JOIN targets the same way, and those indices are concrete (#144109 review).
     */
    static void checkPartialNonKeywordWithLoad(LogicalPlan plan, @Nullable AnalyzerContext context, Failures failures) {
        if (context == null || context.unmappedResolution() != UnmappedResolution.LOAD) {
            return;
        }
        Set<String> usedFieldNames = new HashSet<>();
        plan.forEachDown(LogicalPlan.class, p -> {
            if (p instanceof EsRelation) {
                return;
            }
            p.forEachExpression(FieldAttribute.class, fa -> usedFieldNames.add(fa.name()));
        });

        List<IndexResolution> resolutions = new ArrayList<>();
        plan.forEachDown(EsRelation.class, rel -> {
            if (rel.indexMode() == IndexMode.LOOKUP) {
                return;
            }
            IndexResolution r = context.indexResolution().get(new IndexPattern(rel.source(), rel.indexPattern()));
            if (r != null) {
                resolutions.add(r);
            }
        });

        SortedSet<String> offending = offendingFieldNames(usedFieldNames, resolutions);
        if (offending.isEmpty() == false) {
            failures.add(
                fail(
                    plan,
                    "Loading partially mapped non-KEYWORD fields is not yet supported. "
                        + "unmapped_fields=\"load\" is not supported when the query involves partially mapped fields "
                        + "that are not KEYWORD: {}. "
                        + "Use unmapped_fields=\"nullify\" or unmapped_fields=\"fail\", or ensure the field is mapped as KEYWORD "
                        + "in all indices.",
                    String.join(", ", offending)
                )
            );
        }
    }

    static SortedSet<String> offendingFieldNames(Set<String> usedFieldNames, List<IndexResolution> resolutions) {
        TreeSet<String> offending = new TreeSet<>();
        for (String fieldName : usedFieldNames) {
            for (IndexResolution resolution : resolutions) {
                if (resolution.isValid() == false) {
                    continue;
                }
                EsIndex index = resolution.get();
                if (index.isPartiallyUnmappedField(fieldName) == false) {
                    continue;
                }
                EsField field = resolveFieldByPath(index.mapping(), fieldName);
                if (field == null) {
                    continue;
                }
                if (field instanceof InvalidMappedField) {
                    offending.add(fieldName);
                    break;
                } else if (field.getDataType() != KEYWORD) {
                    offending.add(fieldName);
                    break;
                }
            }
        }
        return offending;
    }

    /**
     * Resolve a dotted field path (e.g. "a.b.c") in the given root mapping.
     * Returns the leaf EsField or null if any segment is missing.
     */
    static EsField resolveFieldByPath(Map<String, EsField> root, String path) {
        if (path == null || path.isEmpty()) {
            return null;
        }
        String[] segments = path.split("\\.");
        Map<String, EsField> current = root;
        EsField field = null;
        for (int i = 0; i < segments.length; i++) {
            field = current.get(segments[i]);
            if (field == null) {
                return null;
            }
            if (i < segments.length - 1) {
                Map<String, EsField> props = field.getProperties();
                if (props == null || props.isEmpty()) {
                    return null;
                }
                current = props;
            }
        }
        return field;
    }
}
