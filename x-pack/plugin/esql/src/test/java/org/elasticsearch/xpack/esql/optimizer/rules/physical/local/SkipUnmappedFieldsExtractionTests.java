/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsAttribute;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.HashMap;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for {@link SkipUnmappedFieldsExtraction}: when the shard is fully mapped for the query's pattern, the rule pulls
 * the synthetic {@link UnmappedFieldsAttribute} out of the {@link FieldExtractExec} and replaces it with a null
 * {@link EvalExec}, so the {@code _source} read is skipped; otherwise the plan is left untouched.
 */
public class SkipUnmappedFieldsExtractionTests extends ESTestCase {

    public void testNullifiesUnmappedFieldsWhenSkippable() {
        FieldAttribute mapped = fieldAttribute("mapped", DataType.LONG);
        UnmappedFieldsAttribute unmapped = new UnmappedFieldsAttribute(Source.EMPTY, UnmappedFieldsPattern.ALL);
        EsQueryExec leaf = esQueryExec();
        FieldExtractExec extract = new FieldExtractExec(
            Source.EMPTY,
            leaf,
            List.of(mapped, unmapped),
            MappedFieldType.FieldExtractPreference.NONE
        );

        PhysicalPlan result = applyRule(extract, searchStats(true));

        assertThat(result, instanceOf(FieldExtractExec.class));
        FieldExtractExec optimized = (FieldExtractExec) result;
        // The unmapped attribute is removed from the extraction, leaving only the genuinely mapped field.
        assertThat(optimized.attributesToExtract(), contains((Attribute) mapped));
        assertThat(optimized.child(), instanceOf(EvalExec.class));
        EvalExec eval = (EvalExec) optimized.child();
        assertThat(eval.fields(), contains(instanceOf(Alias.class)));
        Alias nullified = eval.fields().getFirst();
        // Same name and id so the coordinator's expansion still finds the (now all-null) column and drops it.
        assertThat(nullified.name(), is(unmapped.name()));
        assertThat(nullified.id(), is(unmapped.id()));
        assertThat(nullified.child(), instanceOf(Literal.class));
        Literal literal = (Literal) nullified.child();
        assertThat(literal.value(), nullValue());
        assertThat(literal.dataType(), is(DataType.KEYWORD));
        assertThat(eval.child(), sameInstance(leaf));
    }

    public void testLeavesPlanUntouchedWhenNotSkippable() {
        FieldAttribute mapped = fieldAttribute("mapped", DataType.LONG);
        UnmappedFieldsAttribute unmapped = new UnmappedFieldsAttribute(Source.EMPTY, UnmappedFieldsPattern.ALL);
        FieldExtractExec extract = new FieldExtractExec(
            Source.EMPTY,
            esQueryExec(),
            List.of(mapped, unmapped),
            MappedFieldType.FieldExtractPreference.NONE
        );

        PhysicalPlan result = applyRule(extract, searchStats(false));

        assertThat(result, sameInstance(extract));
    }

    public void testLeavesPlanUntouchedWithoutUnmappedFieldsAttribute() {
        FieldAttribute mapped = fieldAttribute("mapped", DataType.LONG);
        FieldExtractExec extract = new FieldExtractExec(
            Source.EMPTY,
            esQueryExec(),
            List.of(mapped),
            MappedFieldType.FieldExtractPreference.NONE
        );

        // Even when the stats say a skip is possible, there is nothing to skip.
        PhysicalPlan result = applyRule(extract, searchStats(true));

        assertThat(result, sameInstance(extract));
    }

    private static PhysicalPlan applyRule(PhysicalPlan plan, SearchStats searchStats) {
        LocalPhysicalOptimizerContext context = new LocalPhysicalOptimizerContext(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(true),
            TEST_CFG,
            FoldContext.small(),
            searchStats
        );
        return new SkipUnmappedFieldsExtraction().apply(plan, context);
    }

    private static SearchStats searchStats(boolean canSkip) {
        return new EsqlTestUtils.TestSearchStats() {
            @Override
            public boolean canSkipUnmappedFieldsExtraction(UnmappedFieldsPattern pattern) {
                return canSkip;
            }
        };
    }

    private static EsQueryExec esQueryExec() {
        return new EsQueryExec(Source.EMPTY, "test", IndexMode.STANDARD, List.of(), null, null, null, List.of());
    }

    private static FieldAttribute fieldAttribute(String name, DataType type) {
        return new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            name,
            new EsField(name, type, new HashMap<>(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }
}
