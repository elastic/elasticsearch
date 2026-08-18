/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;


/**
 * Handles {@code METADATA} output for views.
 */
public final class ViewMetadataFieldRewriter {

    private ViewMetadataFieldRewriter() {}

    /**
     * Returns the view body wrapped with the rewrites required by the calling query's metadata
     * request, or the body unchanged when no requested metadata field needs special handling.
     *
     * @param viewName the name the view
     * @param viewBody the parsed view body
     * @param outerMetadataFields the metadata fields requested by the referencing {@code FROM}
     */
    public static LogicalPlan rewrite(String viewName, LogicalPlan viewBody, List<NamedExpression> outerMetadataFields) {
        for (NamedExpression metadataField : outerMetadataFields) {
            if (MetadataAttribute.INDEX.equals(metadataField.name())) {
                Source source = viewBody.source();
                return new Eval(source, viewBody, List.of(new Alias(source, MetadataAttribute.INDEX, Literal.keyword(source, viewName))));
            }
        }
        return viewBody;
    }
}
