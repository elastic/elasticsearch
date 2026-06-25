/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session.schema;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.metadata.ViewMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.analysis.InSubqueryResolver;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.SettingsValidationContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.view.InMemoryViewResolver;
import org.elasticsearch.xpack.esql.view.InMemoryViewService;
import org.elasticsearch.xpack.esql.view.PutViewAction;
import org.junit.After;
import org.junit.Before;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_PARSER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;

/**
 * Exercises {@link ViewSchemaProvider} through the {@link SchemaService} umbrella: a {@code VIEW}-typed name in the
 * project lookup is routed to the view provider, which resolves it through the existing {@code ViewResolver} into a
 * {@link ResolvedSchema.View} carrying the view's output schema and its resolved body. Backed by the in-memory view
 * fixtures so the real resolver runs without standing up a cluster.
 */
public class ViewSchemaProviderTests extends ESTestCase {

    private static final InferenceSettings EMPTY_INFERENCE_SETTINGS = new InferenceSettings(Settings.EMPTY);
    private static final List<String> INDICES = List.of("employees", "departments");

    private final QueryParams queryParams = new QueryParams();
    private final ProjectId projectId = ProjectId.DEFAULT;
    private InMemoryViewService viewService;
    private InMemoryViewResolver viewResolver;

    @Before
    public void setup() {
        viewService = InMemoryViewService.makeViewService();
        viewResolver = viewService.getViewResolver();
        for (String index : INDICES) {
            viewService.addIndex(projectId, index);
        }
    }

    @After
    public void teardown() {
        if (viewService != null) {
            viewService.close();
        }
    }

    public void testViewNameDispatchesToViewProviderAndResolvesToItsSchema() {
        addView("dept_view", "FROM departments | KEEP dept_id");

        SchemaService service = new SchemaService(List.of(new ViewSchemaProvider(viewResolver)));
        ProjectMetadata project = projectWith(Map.of("dept_view", "FROM departments | KEEP dept_id"));

        PlainActionFuture<List<ResolvedSchema>> future = new PlainActionFuture<>();
        service.resolveSchema(viewSchemaContext(), project, List.of("dept_view"), future);
        List<ResolvedSchema> resolved = future.actionGet();

        assertThat(resolved, hasSize(1));
        ResolvedSchema.View view = as(resolved.getFirst(), ResolvedSchema.View.class);
        assertEquals("dept_view", view.name());
        // The body is the resolved plan of `FROM departments | KEEP dept_id`; its output is the view's schema — the
        // same attribute list the rest of the pipeline resolves against. Pre-analysis the attribute is the unresolved
        // `dept_id` the KEEP projects, so we assert on the schema's shape (one column named dept_id), not on its type.
        assertThat(view.schema().stream().map(Attribute::name).toList(), contains("dept_id"));
        assertNotNull(view.implementation());
        // No IN subquery should be left dangling in the resolved body.
        InSubqueryResolver.verify(view.implementation());
    }

    public void testViewProviderHandlesOnlyTheViewKind() {
        ViewSchemaProvider provider = new ViewSchemaProvider(viewResolver);
        assertEquals(EnumSet.of(IndexAbstraction.Type.VIEW), provider.handles());
    }

    private SchemaContext viewSchemaContext() {
        // The view path needs only the parser and (CPS) project routing from the context; the rest is unused here.
        return new SchemaContext(null, null, null, null, false, Set.of(), null, null, this::parse, null);
    }

    private LogicalPlan parse(String query, String viewName) {
        return TEST_PARSER.parseView(query, queryParams, new SettingsValidationContext(false, false), EMPTY_INFERENCE_SETTINGS, viewName)
            .plan();
    }

    private void addView(String name, String query) {
        PutViewAction.Request request = new PutViewAction.Request(TimeValue.ONE_MINUTE, TimeValue.ONE_MINUTE, new View(name, query));
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        viewService.putView(projectId, request, future);
        future.actionGet();
    }

    /** A project whose lookup classifies the given views as {@link IndexAbstraction.Type#VIEW} so dispatch routes them here. */
    private ProjectMetadata projectWith(Map<String, String> views) {
        Map<String, View> viewDefs = new HashMap<>();
        views.forEach((name, query) -> viewDefs.put(name, new View(name, query)));
        ProjectMetadata.Builder builder = ProjectMetadata.builder(projectId).putCustom(ViewMetadata.TYPE, new ViewMetadata(viewDefs));
        for (String index : INDICES) {
            builder.put(
                IndexMetadata.builder(index)
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .build()
                    )
                    .build(),
                false
            );
        }
        return builder.build();
    }
}
