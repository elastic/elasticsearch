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
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Regression test for the ordering between {@link PruneUnusedEvalFields} and {@link PushLimitToSource}
 * inside the iterative "Push to ES" batch ({@code LocalPhysicalPlanOptimizer#rules}).
 * <p>
 * Once {@link PushFiltersToSource} fully pushes a {@code WHERE} that referenced an {@code EVAL}-produced
 * alias, it leaves behind a dead {@link EvalExec} sitting directly under the (still unpushed)
 * {@link LimitExec} for the query's implicit/explicit row limit. {@link PushLimitToSource} only pushes a
 * {@link LimitExec} whose immediate child is an {@link EsQueryExec} (or {@code ExchangeExec} wrapping
 * one); with the dead {@link EvalExec} in between it never fires. Only after {@link PruneUnusedEvalFields}
 * removes that {@link EvalExec} does the {@link LimitExec} sit directly above the {@link EsQueryExec} and
 * become pushable &mdash; which is why both rules must live in the same repeating batch, with
 * {@link PruneUnusedEvalFields} getting a chance to run before a later pass of {@link PushLimitToSource}.
 * This test drives the two rules directly (mirroring one extra pass of the batch's do-while loop) rather
 * than the whole {@code LocalPhysicalPlanOptimizer}, since the classification/pushdown logic that produces
 * this shape is already covered by {@link PushFiltersToSourceTests} and {@link PruneUnusedEvalFieldsTests}.
 */
public class PruneUnusedEvalFieldsLimitPushdownTests extends ESTestCase {

    private static final Source SRC = Source.EMPTY;

    /**
     * Mirrors the post-{@code PushFiltersToSource} shape of
     * {@code FROM test | EVAL ext = field_extract(root, "key") | WHERE ext == "val" | KEEP id}: the
     * {@code WHERE} is gone, but the dead {@code ext} eval still sits between the implicit row {@code LIMIT}
     * and the source. A single pass of {@link PushLimitToSource} cannot push the limit through the dead
     * eval, but a second pass &mdash; after {@link PruneUnusedEvalFields} drops it &mdash; can.
     */
    public void testLimitPushesOnceDeadEvalIsPruned() {
        FieldAttribute id = intField("id");
        FieldAttribute root = flattenedRoot("root");
        EsQueryExec source = source(List.of(id, root));

        Alias ext = new Alias(SRC, "ext", new FieldExtract(SRC, root, Literal.keyword(SRC, "key")));
        EvalExec eval = new EvalExec(SRC, source, List.of(ext));
        LimitExec limit = new LimitExec(SRC, eval, Literal.integer(SRC, 1000), null);
        ProjectExec project = new ProjectExec(SRC, limit, List.of(id));

        // First pass: PushLimitToSource cannot see through the dead EvalExec.
        PhysicalPlan afterFirstLimitPass = new PushLimitToSource().apply(project);
        assertThat(afterFirstLimitPass, instanceOf(ProjectExec.class));
        assertThat(((ProjectExec) afterFirstLimitPass).child(), sameInstance(limit));

        // PruneUnusedEvalFields drops the now-dead "ext" eval.
        PhysicalPlan afterPrune = new PruneUnusedEvalFields().apply(afterFirstLimitPass);
        ProjectExec projectAfterPrune = as(afterPrune, ProjectExec.class);
        LimitExec limitAfterPrune = as(projectAfterPrune.child(), LimitExec.class);
        assertThat(limitAfterPrune.child(), sameInstance(source));

        // Second pass: with the dead eval gone, PushLimitToSource can now push the limit into the source.
        PhysicalPlan afterSecondLimitPass = new PushLimitToSource().apply(afterPrune);
        ProjectExec finalProject = as(afterSecondLimitPass, ProjectExec.class);
        EsQueryExec finalSource = as(finalProject.child(), EsQueryExec.class);
        assertThat(finalSource.limit(), equalTo(Literal.integer(SRC, 1000)));
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

    private static FieldAttribute flattenedRoot(String name) {
        return field(name, DataType.FLATTENED);
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(SRC, name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }
}
