/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session.schema;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.View;
import org.elasticsearch.xpack.esql.view.ViewResolver;

import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;

import static org.elasticsearch.index.IndexMode.STANDARD;

/**
 * Schema for views. A view's "schema" is the output of its stored query, so it resolves by <em>expanding</em>
 * the view into that query (a plan rewrite) rather than fetching a flat field list.
 * <p>
 * The umbrella entry ({@link #resolveSchema}) reuses the existing {@link ViewResolver} that the first-class-{@code View}
 * keystone already drives: for each view name it runs view resolution over a single-name {@code FROM <view>} and pulls
 * the resulting first-class {@link View} node out of the rewrite, returning {@link ResolvedSchema.View} with the view's
 * output attributes as its schema and the resolved body plan as its implementation. This is the same body the existing
 * {@code EsqlSession} view-resolution call site produces, so the schema here is obtained behaviour-identically — the
 * output attributes the rest of the pipeline already resolves against — rather than via a new analyze-the-body path.
 * <p>
 * The view-resolution inputs (the {@code (query, viewName) -> plan} parser and the project routing) are read from the
 * {@link SchemaContext}, because they are query-scoped and not available on the abstraction-kind dispatch signature.
 * View authorization remains enforced upstream in the resolve-views transport action, not here.
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

    @Override
    public void resolveSchema(
        SchemaContext ctx,
        ProjectMetadata projectMetadata,
        List<String> names,
        ActionListener<List<ResolvedSchema>> listener
    ) {
        BiFunction<String, String, LogicalPlan> parser = ctx == null ? null : ctx.viewParser();
        if (parser == null) {
            listener.onFailure(new IllegalStateException("view schema resolution requires a view parser on the SchemaContext"));
            return;
        }
        String projectRouting = ctx.projectRouting();
        var grouped = new GroupedActionListener<ResolvedSchema>(names.size(), listener.map(perName -> perName.stream().toList()));
        for (String name : names) {
            resolveOne(name, projectRouting, parser, grouped);
        }
    }

    /**
     * Resolve a single view name to its {@link ResolvedSchema.View} by running the existing view rewrite over a
     * single-name {@code FROM <view>} relation, then locating the first-class {@link View} node the rewrite produced.
     */
    private void resolveOne(
        String name,
        String projectRouting,
        BiFunction<String, String, LogicalPlan> parser,
        ActionListener<ResolvedSchema> listener
    ) {
        LogicalPlan from = new UnresolvedRelation(Source.EMPTY, new IndexPattern(Source.EMPTY, name), false, List.of(), STANDARD, null);
        viewResolver.replaceViews(from, projectRouting, parser, listener.map(result -> {
            View view = findView(result.plan(), name);
            // The body the umbrella returns: the View node's resolved body when the rewrite kept a boundary, or the
            // rewritten plan itself for a single-pattern view the resolver compacts into a bare relation. The schema is
            // that plan's output — the same attribute list the existing pipeline resolves against post-analysis.
            LogicalPlan body = view != null ? view.body() : result.plan();
            LogicalPlan schemaCarrier = view != null ? view : body;
            return new ResolvedSchema.View(name, schemaCarrier.output(), body);
        }));
    }

    /** Find the first-class {@link View} node for {@code name} in the rewritten plan, or {@code null} if it was compacted away. */
    private static View findView(LogicalPlan plan, String name) {
        var found = plan.collectFirstChildren(p -> p instanceof View v && Objects.equals(v.viewName(), name));
        return found.isEmpty() ? null : (View) found.getFirst();
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
