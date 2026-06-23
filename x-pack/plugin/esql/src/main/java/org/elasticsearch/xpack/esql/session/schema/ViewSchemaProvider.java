/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session.schema;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.view.ViewResolver;

import java.util.EnumSet;
import java.util.function.BiFunction;

/**
 * Schema for views. A view's "schema" is the output of its stored query, so it resolves by <em>expanding</em>
 * the view into that query (a plan rewrite) rather than fetching a flat field list. This is asymmetric with the
 * other kinds in two ways the umbrella does not yet erase: it runs earlier in the pipeline (before pre-analysis),
 * and view authorization is enforced upstream in the resolve-views transport action, not here. The provider
 * forwards to {@link ViewResolver} verbatim.
 */
final class ViewSchemaProvider implements AbstractionSchemaProvider {

    private final ViewResolver viewResolver;

    ViewSchemaProvider(ViewResolver viewResolver) {
        this.viewResolver = viewResolver;
    }

    @Override
    public EnumSet<IndexAbstraction.Type> handles() {
        return EnumSet.of(IndexAbstraction.Type.VIEW);
    }

    void replaceViews(
        LogicalPlan plan,
        String projectRouting,
        BiFunction<String, String, LogicalPlan> parser,
        ActionListener<ViewResolver.ViewResolutionResult> listener
    ) {
        viewResolver.replaceViews(plan, projectRouting, parser, listener);
    }
}
