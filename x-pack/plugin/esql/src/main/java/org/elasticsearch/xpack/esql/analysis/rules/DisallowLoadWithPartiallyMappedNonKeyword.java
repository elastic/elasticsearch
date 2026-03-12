/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.AnalyzerRules;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.InvalidMappedField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * Fails the query when {@code SET unmapped_fields="load"} is used and any index has a field that is
 * partially mapped (present in some indices, unmapped in others) and whose type where it is mapped
 * is not KEYWORD. This is a temporary restriction until semantics are finalized.
 */
public class DisallowLoadWithPartiallyMappedNonKeyword extends AnalyzerRules.ParameterizedAnalyzerRule<LogicalPlan, AnalyzerContext> {

    @Override
    protected LogicalPlan rule(LogicalPlan plan, AnalyzerContext context) {
        if (context.unmappedResolution() != UnmappedResolution.LOAD) {
            return plan;
        }
        Set<String> offending = new TreeSet<>();
        collectOffendingFromResolutions(context.indexResolution().values(), offending);
        collectOffendingFromResolutions(context.lookupResolution().values(), offending);
        if (offending.isEmpty() == false) {
            throw new VerificationException(
                "unmapped_fields=\"load\" is not supported when the query involves partially mapped fields that are not KEYWORD: %s. "
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
