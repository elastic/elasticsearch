/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.FieldExtract;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for {@link PruneUnusedEvalFields}, which removes the residual {@link EvalExec} fields
 * that {@link PushFiltersToSource} leaves behind once a {@code WHERE} referencing an
 * {@code EVAL}-produced alias is fully pushed into Lucene.
 */
public class PruneUnusedEvalFieldsTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    /**
     * Mirrors {@code FROM test | EVAL ext = field_extract(root, "key") | WHERE ext == "val" | KEEP id}
     * after pushdown: the filter is gone, {@code id} is projected, and the only remaining eval field
     * ({@code ext}) is unused. The whole {@link EvalExec} must disappear.
     */
    public void testDropsEvalWhenAllFieldsUnused() {
        FieldAttribute id = intField("id");
        FieldAttribute root = flattenedRoot("root");
        EsQueryExec source = source(List.of(id, root));

        Alias ext = new Alias(SRC, "ext", new FieldExtract(SRC, root, Literal.keyword(SRC, "key")));
        EvalExec eval = new EvalExec(SRC, source, List.of(ext));
        ProjectExec project = new ProjectExec(SRC, eval, List.of(id));

        PhysicalPlan optimized = new PruneUnusedEvalFields().apply(project);

        ProjectExec resultProject = as(optimized, ProjectExec.class);
        // The EvalExec is gone: the project now reads straight from the source.
        assertThat(resultProject.child(), sameInstance(source));
    }

    /**
     * When an {@link EvalExec} has an extra alias that <em>is</em> still referenced, only the unused
     * alias is dropped; the surviving alias keeps a slimmed-down {@link EvalExec}.
     */
    public void testKeepsReferencedEvalFieldDropsUnusedOne() {
        FieldAttribute id = intField("id");
        FieldAttribute root = flattenedRoot("root");
        FieldAttribute msg = keywordField("msg");
        EsQueryExec source = source(List.of(id, root, msg));

        Alias ext = new Alias(SRC, "ext", new FieldExtract(SRC, root, Literal.keyword(SRC, "key")));
        Alias other = new Alias(SRC, "other", new FieldExtract(SRC, root, Literal.keyword(SRC, "other")));
        EvalExec eval = new EvalExec(SRC, source, List.of(ext, other));
        // Only "other" survives downstream.
        ProjectExec project = new ProjectExec(SRC, eval, List.of(other.toAttribute()));

        PhysicalPlan optimized = new PruneUnusedEvalFields().apply(project);

        ProjectExec resultProject = as(optimized, ProjectExec.class);
        EvalExec resultEval = as(resultProject.child(), EvalExec.class);
        assertThat(resultEval.fields(), hasSize(1));
        assertThat(resultEval.fields().get(0).name(), equalTo("other"));
        assertThat(resultEval.child(), sameInstance(source));
    }

    /**
     * A later eval field that feeds a surviving earlier-and-later chain must not be pruned even
     * though nothing above the {@link EvalExec} references the intermediate alias directly.
     */
    public void testKeepsFieldFeedingASurvivingChainedField() {
        FieldAttribute id = intField("id");
        FieldAttribute root = flattenedRoot("root");
        EsQueryExec source = source(List.of(id, root));

        Alias f = new Alias(SRC, "f", new FieldExtract(SRC, root, Literal.keyword(SRC, "key")));
        // "ext" references "f"; only "ext" is projected downstream, so "f" must be retained to feed it.
        Alias ext = new Alias(SRC, "ext", new FieldExtract(SRC, f.toAttribute(), Literal.keyword(SRC, "sub")));
        EvalExec eval = new EvalExec(SRC, source, List.of(f, ext));
        ProjectExec project = new ProjectExec(SRC, eval, List.of(ext.toAttribute()));

        PhysicalPlan optimized = new PruneUnusedEvalFields().apply(project);

        ProjectExec resultProject = as(optimized, ProjectExec.class);
        EvalExec resultEval = as(resultProject.child(), EvalExec.class);
        assertThat(resultEval.fields().stream().map(Alias::name).toList(), contains("f", "ext"));
    }

    /**
     * When nothing is unused, the rule is a no-op and returns the original plan instance.
     */
    public void testNoOpWhenAllFieldsUsed() {
        FieldAttribute id = intField("id");
        FieldAttribute root = flattenedRoot("root");
        EsQueryExec source = source(List.of(id, root));

        Alias ext = new Alias(SRC, "ext", new FieldExtract(SRC, root, Literal.keyword(SRC, "key")));
        EvalExec eval = new EvalExec(SRC, source, List.of(ext));
        ProjectExec project = new ProjectExec(SRC, eval, List.of(id, ext.toAttribute()));

        PhysicalPlan optimized = new PruneUnusedEvalFields().apply(project);

        assertThat(optimized, sameInstance(project));
    }

    private static <T> T as(Object obj, Class<T> type) {
        assertThat(obj, instanceOf(type));
        return type.cast(obj);
    }

    private static EsQueryExec source(List<Attribute> attrs) {
        return new EsQueryExec(SRC, "test", IndexMode.STANDARD, attrs, null, null, null, List.of());
    }

    private static FieldAttribute intField(String name) {
        return field(name, DataType.INTEGER);
    }

    private static FieldAttribute keywordField(String name) {
        return field(name, DataType.KEYWORD);
    }

    private static FieldAttribute flattenedRoot(String name) {
        return field(name, DataType.FLATTENED);
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(SRC, name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }
}
