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
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

public class SkipUnmappedFieldsExtractionTests extends ESTestCase {

    public void testExtractionKeptWhenAShardMayHoldUnmappedFields() {
        FieldExtractExec extract = extract(mapped("mapped"), unmappedFields());
        assertThat(apply(extract, true), sameInstance(extract));
    }

    public void testNothingToSkipWithoutTheUnmappedFieldsColumn() {
        FieldExtractExec extract = extract(mapped("mapped"));
        assertThat(apply(extract, false), sameInstance(extract));
    }

    public void testColumnBecomesNullAndOtherFieldsAreStillExtracted() {
        FieldAttribute mapped = mapped("mapped");
        UnmappedFieldsAttribute unmappedFields = unmappedFields();
        FieldExtractExec extract = extract(mapped, unmappedFields);

        FieldExtractExec remaining = as(apply(extract, false), FieldExtractExec.class);
        assertThat(remaining.attributesToExtract(), contains(mapped));
        assertNullColumn(as(remaining.child(), EvalExec.class), unmappedFields, extract.child());
    }

    public void testExtractionRemovedWhenTheColumnWasAllItExtracted() {
        UnmappedFieldsAttribute unmappedFields = unmappedFields();
        FieldExtractExec extract = extract(unmappedFields);

        assertNullColumn(as(apply(extract, false), EvalExec.class), unmappedFields, extract.child());
    }

    private static void assertNullColumn(EvalExec eval, UnmappedFieldsAttribute unmappedFields, PhysicalPlan expectedChild) {
        assertThat(eval.child(), sameInstance(expectedChild));
        assertThat(eval.fields(), hasSize(1));
        Alias alias = eval.fields().getFirst();
        // The same id, so the references to the column the extraction produced keep resolving.
        assertThat(alias.id(), equalTo(unmappedFields.id()));
        assertThat(alias.name(), equalTo(unmappedFields.name()));
        Literal literal = as(alias.child(), Literal.class);
        assertNull(literal.value());
        assertThat(literal.dataType(), equalTo(DataType.KEYWORD));
    }

    private static PhysicalPlan apply(PhysicalPlan plan, boolean mayHoldUnmappedFields) {
        SearchStats stats = new EsqlTestUtils.TestSearchStats() {
            @Override
            public boolean mayHoldUnmappedFields() {
                return mayHoldUnmappedFields;
            }
        };
        return new SkipUnmappedFieldsExtraction().apply(
            plan,
            new LocalPhysicalOptimizerContext(PlannerSettings.DEFAULTS, new EsqlFlags(true), TEST_CFG, FoldContext.small(), stats)
        );
    }

    private static FieldExtractExec extract(Attribute... attributes) {
        EsQueryExec source = new EsQueryExec(Source.EMPTY, "test", IndexMode.STANDARD, List.of(), null, null, null, List.of());
        return new FieldExtractExec(Source.EMPTY, source, List.of(attributes), MappedFieldType.FieldExtractPreference.NONE);
    }

    private static UnmappedFieldsAttribute unmappedFields() {
        return new UnmappedFieldsAttribute(Source.EMPTY, UnmappedFieldsPattern.ALL);
    }

    private static FieldAttribute mapped(String name) {
        return new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            name,
            new EsField(name, DataType.LONG, new HashMap<>(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }
}
