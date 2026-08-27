/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Three rules govern {@code METADATA} field behavior for views:
 * <ol>
 *   <li>Fields declared in the view body pass through as regular columns.</li>
 *   <li>Fields requested by the outer query but absent from the view body produce {@code null}.</li>
 *   <li>Fields both declared in the body and requested by the outer query are left unchanged.</li>
 * </ol>
 * Rules 1 and 3 require no action. This class enforces rule 2 by prepending an {@code EVAL} that
 * assigns {@code null} for each such field.
 */
public final class ViewMetadataNullInjector {

    private ViewMetadataNullInjector() {}

    /**
     * Rewrites {@code viewBody} to satisfy rule 2: prepends an {@code EVAL} that assigns {@code null}
     * for each outer-requested metadata field not declared in the view body.
     *
     * @param viewBody            the parsed view body
     * @param outerMetadataFields the metadata fields requested by the referencing {@code FROM}
     * @return the rewritten plan, or {@code viewBody} unchanged when no null-fill is needed
     */
    public static LogicalPlan inject(LogicalPlan viewBody, List<NamedExpression> outerMetadataFields) {
        if (outerMetadataFields.isEmpty()) {
            return viewBody;
        }

        Set<String> bodyMetadataNames = collectBodyMetadataFieldNames(viewBody);

        Source source = viewBody.source();
        List<Alias> nullFills = new ArrayList<>();
        for (NamedExpression metadataField : outerMetadataFields) {
            if (bodyMetadataNames.contains(metadataField.name()) == false) {
                nullFills.add(new Alias(source, metadataField.name(), new Literal(source, null, metadataField.dataType())));
            }
        }

        if (nullFills.isEmpty()) {
            return viewBody;
        }
        return new Eval(source, viewBody, nullFills);
    }

    private static Set<String> collectBodyMetadataFieldNames(LogicalPlan plan) {
        Set<String> names = new HashSet<>();
        plan.forEachDown(UnresolvedRelation.class, ur -> ur.metadataFields().forEach(f -> names.add(f.name())));
        return names;
    }
}
