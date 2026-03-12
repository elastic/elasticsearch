/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * Fails the query when {@code SET unmapped_fields="load"} is used and any index has a field that is
 * partially mapped (present in some indices, unmapped in others) and whose type where it is mapped
 * is not KEYWORD. This is a temporary restriction until semantics are finalized.
 * <p>
 * Implemented as an analyzer rule (rather than in {@link org.elasticsearch.xpack.esql.analysis.Verifier})
 * because this check requires index resolution context (e.g. {@link EsIndex#partiallyUnmappedFields()}).
 * All offending field names are collected and reported in a single exception.
 */
public class DisallowLoadWithPartiallyMappedNonKeyword extends AnalyzerRules.ParameterizedAnalyzerRule<LogicalPlan, AnalyzerContext> {

    @Override
    protected boolean skipResolved() {
        return false;  // run on all nodes so we always check indices in the plan
    }

    @Override
    protected LogicalPlan rule(LogicalPlan plan, AnalyzerContext context) {
        if (context.unmappedResolution() != UnmappedResolution.LOAD) {
            return plan;
        }
        // Only check indices that appear in the plan (EsRelation nodes)
        List<IndexResolution> resolutionsInPlan = new ArrayList<>();
        plan.forEachDown(EsRelation.class, relation -> {
            if (relation.indexMode() == IndexMode.LOOKUP) {
                IndexResolution r = context.lookupResolution().get(relation.indexPattern());
                if (r != null) {
                    resolutionsInPlan.add(r);
                }
            } else {
                IndexResolution r = context.indexResolution().get(new IndexPattern(relation.source(), relation.indexPattern()));
                if (r != null) {
                    resolutionsInPlan.add(r);
                }
            }
        });
        Set<String> offending = new TreeSet<>();
        collectOffendingFromResolutions(resolutionsInPlan, offending);
        if (offending.isEmpty() == false) {
            throw new VerificationException(
                "unmapped_fields=\"load\" is not supported when the query involves partially mapped fields that are not KEYWORD: {}. "
                    + "Use unmapped_fields=\"nullify\" or unmapped_fields=\"fail\", or ensure the field is mapped as KEYWORD in all indices.",
                String.join(", ", offending)
            );
        }
        return plan;
    }

    private static void collectOffendingFromResolutions(Collection<IndexResolution> resolutions, Set<String> offending) {
        for (IndexResolution resolution : resolutions) {
            if (resolution.isValid() == false) {
                continue;
            }
            EsIndex index = resolution.get();
            for (String fieldName : index.partiallyUnmappedFields()) {
                EsField field = resolveFieldByPath(index.mapping(), fieldName);
                if (field == null) {
                    continue;
                }
                if (field instanceof InvalidMappedField) {
                    offending.add(fieldName);
                } else if (field.getDataType() != KEYWORD) {
                    offending.add(fieldName);
                }
            }
        }
    }

    /**
     * Resolve a dotted field path (e.g. "a.b.c") in the given root mapping.
     * Returns the leaf EsField or null if any segment is missing.
     */
    private static EsField resolveFieldByPath(Map<String, EsField> root, String path) {
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
