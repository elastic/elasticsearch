/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBigArrayBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBigArrayBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.local.CopyingLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.session.SessionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.LEFT;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SubqueryJoinPlanTests extends ESTestCase {

    public void testOutputUsesRightReferencesAndPreservesLeftJoinKey() {
        FieldAttribute key = getFieldAttribute("key", DataType.INTEGER);
        FieldAttribute leftValue = getFieldAttribute("value", DataType.INTEGER);
        FieldAttribute rightValue = getFieldAttribute("value", DataType.INTEGER);
        SubqueryJoin join = new SubqueryJoin(
            Source.EMPTY,
            relation(List.of(key, leftValue)),
            relation(List.of(key, rightValue)),
            List.of(key)
        );

        assertThat(join.config().type(), sameInstance(JoinTypes.LEFT));
        assertThat(join.output().stream().filter(attribute -> attribute.name().equals("key")).toList(), contains(key));

        Attribute outputValue = join.output().stream().filter(attribute -> attribute.name().equals("value")).findFirst().orElseThrow();
        assertThat(outputValue, instanceOf(ReferenceAttribute.class));
        assertThat(outputValue.id(), equalTo(rightValue.id()));
    }

    public void testInnerJoinOutputUsesRightReferencesAndPreservesLeftJoinKey() {
        FieldAttribute key = getFieldAttribute("key", DataType.INTEGER);
        FieldAttribute leftValue = getFieldAttribute("value", DataType.INTEGER);
        FieldAttribute rightValue = getFieldAttribute("value", DataType.INTEGER);
        SubqueryJoin join = new SubqueryJoin(
            Source.EMPTY,
            relation(List.of(key, leftValue)),
            relation(List.of(key, rightValue)),
            JoinTypes.INNER,
            List.of(key)
        );

        assertThat(join.config().type(), sameInstance(JoinTypes.INNER));
        assertThat(join.output().stream().filter(attribute -> attribute.name().equals("key")).toList(), contains(key));
        Attribute outputValue = join.output().stream().filter(attribute -> attribute.name().equals("value")).findFirst().orElseThrow();
        assertThat(outputValue, instanceOf(ReferenceAttribute.class));
        assertThat(outputValue.id(), equalTo(rightValue.id()));
    }

    public void testRebuildsRetainTheSubqueryJoinType() {
        FieldAttribute key = getFieldAttribute("key", DataType.INTEGER);
        LocalRelation left = relation(List.of(key));
        LocalRelation right = relation(List.of(key));
        SubqueryJoin join = new SubqueryJoin(Source.EMPTY, left, right, List.of(key));

        assertThat(join.replaceChildren(left, right), instanceOf(SubqueryJoin.class));
        assertThat(join.withConfig(join.config()), instanceOf(SubqueryJoin.class));
    }

    public void testPostAnalysisVerificationAllowsCompatibleKeyTypes() {
        for (SubqueryJoin join : List.of(
            joinForKeyTypes(DataType.INTEGER, DataType.DOUBLE),
            joinForKeyTypes(DataType.KEYWORD, DataType.TEXT),
            joinForKeyTypes(DataType.NULL, DataType.KEYWORD)
        )) {
            Failures failures = new Failures();
            join.postAnalysisVerification(failures);
            assertFalse(failures.toString(), failures.hasFailures());
        }
    }

    public void testPostAnalysisVerificationRejectsDateAndDateNanosKeys() {
        for (SubqueryJoin join : List.of(
            joinForKeyTypes(DataType.DATETIME, DataType.DATE_NANOS),
            joinForKeyTypes(DataType.DATE_NANOS, DataType.DATETIME)
        )) {
            Failures failures = new Failures();
            join.postAnalysisVerification(failures);
            assertThat(failures.toString(), containsString("is incompatible with right field"));
        }
    }

    public void testPostAnalysisVerificationRejectsUnsupportedRightKeyType() {
        Failures failures = new Failures();
        joinForKeyTypes(DataType.AGGREGATE_METRIC_DOUBLE, DataType.AGGREGATE_METRIC_DOUBLE).postAnalysisVerification(failures);
        assertThat(failures.toString(), containsString("right field [key] of type [AGGREGATE_METRIC_DOUBLE] is not supported"));
    }

    public void testSubqueryPayloadSupportCoversEveryDataType() {
        for (DataType dataType : DataType.values()) {
            boolean expected = switch (dataType) {
                case AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, TDIGEST, DATE_RANGE, DOUBLE_RANGE -> false;
                case UNSUPPORTED, NULL, BOOLEAN, COUNTER_LONG, COUNTER_INTEGER, COUNTER_DOUBLE, LONG, INTEGER, UNSIGNED_LONG, DOUBLE, SHORT,
                    BYTE, FLOAT, HALF_FLOAT, SCALED_FLOAT, KEYWORD, TEXT, DATETIME, DATE_NANOS, IP, VERSION, OBJECT, SOURCE, DATE_PERIOD,
                    TIME_DURATION, GEO_POINT, CARTESIAN_POINT, CARTESIAN_SHAPE, GEO_SHAPE, GEOHASH, GEOTILE, GEOHEX, DOC_DATA_TYPE,
                    TSID_DATA_TYPE, PARTIAL_AGG, HISTOGRAM, DENSE_VECTOR, FLATTENED -> true;
            };
            assertEquals(
                "Unexpected subquery payload support for [" + dataType + "]",
                expected,
                SubqueryJoin.supportsSubqueryPayload(dataType)
            );
        }
    }

    public void testPostOptimizationVerificationRejectsUnsupportedPayloadTypes() {
        for (DataType dataType : List.of(
            DataType.AGGREGATE_METRIC_DOUBLE,
            DataType.EXPONENTIAL_HISTOGRAM,
            DataType.TDIGEST,
            DataType.DATE_RANGE,
            DataType.DOUBLE_RANGE
        )) {
            Failures failures = new Failures();
            joinWithPayload(dataType).postOptimizationVerification(failures);
            assertThat(failures.toString(), containsString("JOIN subquery cannot return column [payload]"));
        }

        Failures supportedPayload = new Failures();
        joinWithPayload(DataType.LONG).postOptimizationVerification(supportedPayload);
        assertFalse(supportedPayload.toString(), supportedPayload.hasFailures());
    }

    public void testIsNotSerialized() {
        FieldAttribute key = getFieldAttribute("key", DataType.INTEGER);
        SubqueryJoin join = new SubqueryJoin(Source.EMPTY, relation(List.of(key)), relation(List.of(key)), List.of(key));

        expectThrows(UnsupportedOperationException.class, () -> join.writeTo((StreamOutput) null));
        expectThrows(UnsupportedOperationException.class, join::getWriteableName);
    }

    public void testInliningFiltersNullAndMultivaluedKeysAndRetainsRepeatedRows() {
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        ReferenceAttribute leftKey = attribute("key", DataType.INTEGER);
        ReferenceAttribute rightKey = attribute("key", DataType.INTEGER);
        ReferenceAttribute payload = attribute("payload", DataType.INTEGER);
        SubqueryJoin join = subqueryJoin(leftKey, rightKey, payload, ExecuteLocation.ANY);

        Page inputPage;
        try (IntBlock.Builder keys = blockFactory.newIntBlockBuilder(4); IntBlock.Builder values = blockFactory.newIntBlockBuilder(4)) {
            keys.appendInt(1);
            keys.appendNull();
            keys.beginPositionEntry();
            keys.appendInt(2);
            keys.appendInt(3);
            keys.endPositionEntry();
            keys.appendInt(1);
            values.appendInt(10);
            values.appendInt(20);
            values.appendInt(30);
            values.appendInt(40);
            inputPage = new Page(keys.build(), values.build());
        }
        AtomicReference<Page> pageHolder = new AtomicReference<>(inputPage);
        Block inputKeyBlock = inputPage.getBlock(0);
        Block inputPayloadBlock = inputPage.getBlock(1);
        LocalRelation materialized = new LocalRelation(Source.EMPTY, List.of(rightKey, payload), LocalSupplier.of(inputPage));

        LogicalPlan inlined = join.inlineSubqueryResult(materialized, resultContext(blockFactory, pageHolder, ByteSizeValue.ofMb(100)));
        MaterializedSubqueryJoin rewritten = as(inlined, MaterializedSubqueryJoin.class);
        assertThat(rewritten.executesOn(), equalTo(ExecuteLocation.ANY));
        assertThat(rewritten.config().rightFields(), contains(rightKey));
        LocalRelation right = as(rewritten.right(), LocalRelation.class);
        assertThat(right.output(), contains(rightKey, payload));
        assertNotSame(inputPage, pageHolder.get());
        assertTrue(inputKeyBlock.isReleased());
        assertTrue(inputPayloadBlock.isReleased());
        Page normalizedPage = pageHolder.get();
        try {
            assertThat(normalizedPage.getPositionCount(), equalTo(2));
            IntBlock normalizedKeys = normalizedPage.getBlock(0);
            IntBlock normalizedPayload = normalizedPage.getBlock(1);
            assertThat(normalizedKeys.getInt(0), equalTo(1));
            assertThat(normalizedKeys.getInt(1), equalTo(1));
            assertThat(normalizedPayload.getInt(0), equalTo(10));
            assertThat(normalizedPayload.getInt(1), equalTo(40));
        } finally {
            pageHolder.set(null);
            normalizedPage.releaseBlocks();
        }

        assertThat(
            drainWarnings(),
            contains(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded",
                "Line -1:-1: java.lang.IllegalArgumentException: JOIN subquery encountered multi-value"
            )
        );
    }

    public void testInnerJoinWithEmptyRightSideProducesNoRows() {
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        ReferenceAttribute leftKey = attribute("key", DataType.INTEGER);
        ReferenceAttribute rightKey = attribute("key", DataType.INTEGER);
        ReferenceAttribute payload = attribute("payload", DataType.LONG);
        SubqueryJoin join = new SubqueryJoin(
            Source.EMPTY,
            relation(List.of(leftKey)),
            relation(List.of(rightKey, payload)),
            JoinTypes.INNER,
            List.of(leftKey),
            List.of(rightKey),
            null,
            ExecuteLocation.ANY
        );

        Page inputPage;
        try (IntBlock.Builder keys = blockFactory.newIntBlockBuilder(0); LongBlock.Builder values = blockFactory.newLongBlockBuilder(0)) {
            inputPage = new Page(keys.build(), values.build());
        }
        AtomicReference<Page> pageHolder = new AtomicReference<>(inputPage);
        LocalRelation materialized = new LocalRelation(Source.EMPTY, List.of(rightKey, payload), LocalSupplier.of(inputPage));

        Project project = as(
            join.inlineSubqueryResult(materialized, resultContext(blockFactory, pageHolder, ByteSizeValue.ofMb(100))),
            Project.class
        );
        Eval eval = as(project.child(), Eval.class);
        Filter filter = as(eval.child(), Filter.class);
        Literal condition = as(filter.condition(), Literal.class);
        assertThat(condition.value(), equalTo(false));
        assertThat(project.output(), equalTo(join.output()));
        assertNull(pageHolder.get());
    }

    public void testInliningFiltersCompositeKeysWhenAnyComponentIsNullOrMultivalued() {
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        ReferenceAttribute leftTenant = attribute("tenant", DataType.INTEGER);
        ReferenceAttribute leftKey = attribute("key", DataType.KEYWORD);
        ReferenceAttribute rightTenant = attribute("tenant", DataType.INTEGER);
        ReferenceAttribute rightKey = attribute("key", DataType.KEYWORD);
        ReferenceAttribute payload = attribute("payload", DataType.LONG);
        SubqueryJoin join = new SubqueryJoin(
            Source.EMPTY,
            relation(List.of(leftTenant, leftKey)),
            relation(List.of(rightTenant, rightKey, payload)),
            LEFT,
            List.of(leftTenant, leftKey),
            List.of(rightTenant, rightKey),
            null,
            ExecuteLocation.ANY
        );

        Page inputPage;
        try (
            IntBlock.Builder tenants = blockFactory.newIntBlockBuilder(4);
            BytesRefBlock.Builder keys = blockFactory.newBytesRefBlockBuilder(4);
            LongBlock.Builder values = blockFactory.newLongBlockBuilder(4)
        ) {
            tenants.appendInt(1);
            keys.appendBytesRef(new BytesRef("a"));
            values.appendLong(10);
            tenants.appendInt(1);
            keys.appendNull();
            values.appendLong(20);
            tenants.appendInt(1);
            keys.beginPositionEntry();
            keys.appendBytesRef(new BytesRef("b"));
            keys.appendBytesRef(new BytesRef("c"));
            keys.endPositionEntry();
            values.appendLong(30);
            tenants.appendInt(2);
            keys.appendBytesRef(new BytesRef("a"));
            values.appendLong(40);
            inputPage = new Page(tenants.build(), keys.build(), values.build());
        }
        AtomicReference<Page> pageHolder = new AtomicReference<>(inputPage);
        LocalRelation materialized = new LocalRelation(Source.EMPTY, List.of(rightTenant, rightKey, payload), LocalSupplier.of(inputPage));

        MaterializedSubqueryJoin rewritten = as(
            join.inlineSubqueryResult(materialized, resultContext(blockFactory, pageHolder, ByteSizeValue.ofMb(100))),
            MaterializedSubqueryJoin.class
        );
        assertThat(rewritten.config().rightFields(), contains(rightTenant, rightKey));
        Page normalizedPage = pageHolder.getAndSet(null);
        try {
            assertThat(normalizedPage.getPositionCount(), equalTo(2));
            assertThat(((IntBlock) normalizedPage.getBlock(0)).getInt(0), equalTo(1));
            assertThat(((IntBlock) normalizedPage.getBlock(0)).getInt(1), equalTo(2));
            assertThat(((LongBlock) normalizedPage.getBlock(2)).getLong(0), equalTo(10L));
            assertThat(((LongBlock) normalizedPage.getBlock(2)).getLong(1), equalTo(40L));
        } finally {
            normalizedPage.releaseBlocks();
        }
        assertThat(
            drainWarnings(),
            contains(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded",
                "Line -1:-1: java.lang.IllegalArgumentException: JOIN subquery encountered multi-value"
            )
        );
    }

    public void testInlineStatsSubplanCopiesMaterializedSubqueryRows() {
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        ReferenceAttribute leftKey = attribute("key", DataType.INTEGER);
        ReferenceAttribute rightKey = attribute("key", DataType.INTEGER);
        ReferenceAttribute payload = attribute("payload", DataType.LONG);
        Page originalPage;
        try (IntBlock.Builder keys = blockFactory.newIntBlockBuilder(2); LongBlock.Builder values = blockFactory.newLongBlockBuilder(2)) {
            keys.appendInt(1);
            keys.appendInt(1);
            values.appendLong(10);
            values.appendLong(11);
            originalPage = new Page(keys.build(), values.build());
        }
        LocalRelation left = relation(List.of(leftKey));
        LocalRelation right = new LocalRelation(Source.EMPTY, List.of(rightKey, payload), LocalSupplier.of(originalPage));
        MaterializedSubqueryJoin materializedSubqueryJoin = new MaterializedSubqueryJoin(
            Source.EMPTY,
            left,
            right,
            LEFT,
            List.of(leftKey),
            List.of(rightKey),
            null,
            ExecuteLocation.ANY
        );
        InlineJoin inlineJoin = new InlineJoin(Source.EMPTY, left, materializedSubqueryJoin, LEFT, List.of(), List.of());

        InlineJoin.LogicalPlanTuple subPlans = InlineJoin.firstSubPlan(inlineJoin, Set.of());
        MaterializedSubqueryJoin copiedSubqueryJoin = as(subPlans.stubReplacedSubPlan(), MaterializedSubqueryJoin.class);
        LocalRelation copiedRelation = as(copiedSubqueryJoin.right(), LocalRelation.class);
        assertThat(copiedRelation.supplier(), instanceOf(CopyingLocalSupplier.class));
        Page firstCopy = copiedRelation.supplier().get();
        Page secondCopy = copiedRelation.supplier().get();
        try {
            assertNotSame(originalPage, firstCopy);
            assertNotSame(firstCopy, secondCopy);
            assertNotSame(firstCopy.getBlock(1), secondCopy.getBlock(1));
            assertThat(((LongBlock) firstCopy.getBlock(1)).getLong(0), equalTo(10L));
            assertThat(((LongBlock) secondCopy.getBlock(1)).getLong(1), equalTo(11L));
        } finally {
            firstCopy.releaseBlocks();
            secondCopy.releaseBlocks();
            originalPage.releaseBlocks();
        }
    }

    public void testInliningWidensRightNumericKeysAndRebindsKeyAttribute() {
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        ReferenceAttribute leftKey = attribute("key", DataType.DOUBLE);
        ReferenceAttribute rightKey = attribute("key", DataType.INTEGER);
        ReferenceAttribute payload = attribute("payload", DataType.LONG);
        SubqueryJoin join = subqueryJoin(leftKey, rightKey, payload, ExecuteLocation.COORDINATOR);

        Page inputPage;
        try (IntBlock.Builder keys = blockFactory.newIntBlockBuilder(2); LongBlock.Builder values = blockFactory.newLongBlockBuilder(2)) {
            keys.appendInt(1);
            keys.appendInt(2);
            values.appendLong(10);
            values.appendLong(20);
            inputPage = new Page(keys.build(), values.build());
        }
        AtomicReference<Page> pageHolder = new AtomicReference<>(inputPage);
        Block inputKeyBlock = inputPage.getBlock(0);
        Block inputPayloadBlock = inputPage.getBlock(1);
        LocalRelation materialized = new LocalRelation(Source.EMPTY, List.of(rightKey, payload), LocalSupplier.of(inputPage));

        MaterializedSubqueryJoin rewritten = as(
            join.inlineSubqueryResult(materialized, resultContext(blockFactory, pageHolder, ByteSizeValue.ofMb(100))),
            MaterializedSubqueryJoin.class
        );
        assertThat(rewritten.executesOn(), equalTo(ExecuteLocation.COORDINATOR));
        assertTrue(inputKeyBlock.isReleased());
        // Every row is kept, so the payload block moves to the result page instead of being copied.
        assertSame(inputPayloadBlock, pageHolder.get().getBlock(1));
        assertFalse(inputPayloadBlock.isReleased());
        LocalRelation right = as(rewritten.right(), LocalRelation.class);
        Attribute widenedKey = right.output().get(0);
        assertThat(widenedKey, instanceOf(ReferenceAttribute.class));
        assertThat(widenedKey.dataType(), equalTo(DataType.DOUBLE));
        assertThat(widenedKey.id(), equalTo(rightKey.id()));
        assertSame(widenedKey, rewritten.config().rightFields().get(0));
        DoubleBlock normalizedKeys = pageHolder.get().getBlock(0);
        assertThat(normalizedKeys.getDouble(0), equalTo(1.0));
        assertThat(normalizedKeys.getDouble(1), equalTo(2.0));

        Page normalizedPage = pageHolder.getAndSet(null);
        normalizedPage.releaseBlocks();
        assertTrue(inputPayloadBlock.isReleased());
    }

    public void testInliningWidensLeftKeyAndProjectsAwaySyntheticKey() {
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        ReferenceAttribute leftKey = attribute("key", DataType.INTEGER);
        ReferenceAttribute rightKey = attribute("key", DataType.LONG);
        ReferenceAttribute payload = attribute("payload", DataType.LONG);
        SubqueryJoin join = subqueryJoin(leftKey, rightKey, payload, ExecuteLocation.ANY);

        Page inputPage;
        try (LongBlock.Builder keys = blockFactory.newLongBlockBuilder(1); LongBlock.Builder values = blockFactory.newLongBlockBuilder(1)) {
            keys.appendLong(1);
            values.appendLong(10);
            inputPage = new Page(keys.build(), values.build());
        }
        AtomicReference<Page> pageHolder = new AtomicReference<>(inputPage);
        LocalRelation materialized = new LocalRelation(Source.EMPTY, List.of(rightKey, payload), LocalSupplier.of(inputPage));

        Project project = as(
            join.inlineSubqueryResult(materialized, resultContext(blockFactory, pageHolder, ByteSizeValue.ofMb(100))),
            Project.class
        );
        MaterializedSubqueryJoin rewritten = as(project.child(), MaterializedSubqueryJoin.class);
        Eval cast = as(rewritten.left(), Eval.class);
        assertThat(cast.fields().get(0).child(), instanceOf(ToLong.class));
        assertThat(rewritten.config().leftFields().get(0).name(), equalTo("$$subquery_join_key_0"));
        assertThat(project.output(), equalTo(join.output()));

        pageHolder.getAndSet(null).releaseBlocks();
    }

    public void testNoMatchCreatesNullPayloadAndReleasesMaterializedPage() {
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        ReferenceAttribute leftKey = attribute("key", DataType.INTEGER);
        ReferenceAttribute rightKey = attribute("key", DataType.INTEGER);
        ReferenceAttribute payload = attribute("payload", DataType.LONG);
        SubqueryJoin join = subqueryJoin(leftKey, rightKey, payload, ExecuteLocation.ANY);

        Page inputPage;
        try (IntBlock.Builder keys = blockFactory.newIntBlockBuilder(2); LongBlock.Builder values = blockFactory.newLongBlockBuilder(2)) {
            keys.appendNull();
            keys.beginPositionEntry();
            keys.appendInt(1);
            keys.appendInt(2);
            keys.endPositionEntry();
            values.appendLong(10);
            values.appendLong(20);
            inputPage = new Page(keys.build(), values.build());
        }
        AtomicReference<Page> pageHolder = new AtomicReference<>(inputPage);
        Block inputKeyBlock = inputPage.getBlock(0);
        Block inputPayloadBlock = inputPage.getBlock(1);
        LocalRelation materialized = new LocalRelation(Source.EMPTY, List.of(rightKey, payload), LocalSupplier.of(inputPage));

        Project project = as(
            join.inlineSubqueryResult(materialized, resultContext(blockFactory, pageHolder, ByteSizeValue.ofMb(100))),
            Project.class
        );
        Eval eval = as(project.child(), Eval.class);
        Alias nullPayload = eval.fields().get(0);
        Literal literal = as(nullPayload.child(), Literal.class);
        assertNull(literal.value());
        assertThat(literal.dataType(), equalTo(DataType.LONG));
        assertThat(pageHolder.get(), nullValue());
        assertTrue(inputKeyBlock.isReleased());
        assertTrue(inputPayloadBlock.isReleased());
        assertThat(
            drainWarnings(),
            contains(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded",
                "Line -1:-1: java.lang.IllegalArgumentException: JOIN subquery encountered multi-value"
            )
        );
    }

    public void testNullTypedLeftKeyDoesNotMaterializeRightSide() {
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        ReferenceAttribute leftKey = attribute("key", DataType.NULL);
        ReferenceAttribute rightKey = attribute("key", DataType.INTEGER);
        ReferenceAttribute payload = attribute("payload", DataType.LONG);
        SubqueryJoin join = subqueryJoin(leftKey, rightKey, payload, ExecuteLocation.ANY);

        Page inputPage;
        try (IntBlock.Builder keys = blockFactory.newIntBlockBuilder(1); LongBlock.Builder values = blockFactory.newLongBlockBuilder(1)) {
            keys.appendInt(1);
            values.appendLong(10);
            inputPage = new Page(keys.build(), values.build());
        }
        AtomicReference<Page> pageHolder = new AtomicReference<>(inputPage);
        Block inputKeyBlock = inputPage.getBlock(0);
        LocalRelation materialized = new LocalRelation(Source.EMPTY, List.of(rightKey, payload), LocalSupplier.of(inputPage));

        Project project = as(
            join.inlineSubqueryResult(materialized, resultContext(blockFactory, pageHolder, ByteSizeValue.ofMb(100))),
            Project.class
        );
        assertThat(as(project.child(), Eval.class).fields(), hasSize(1));
        assertThat(pageHolder.get(), nullValue());
        assertTrue(inputKeyBlock.isReleased());
    }

    public void testEmptyRightSideCreatesNullPayload() {
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        ReferenceAttribute leftKey = attribute("key", DataType.INTEGER);
        ReferenceAttribute rightKey = attribute("key", DataType.INTEGER);
        ReferenceAttribute payload = attribute("payload", DataType.LONG);
        SubqueryJoin join = subqueryJoin(leftKey, rightKey, payload, ExecuteLocation.ANY);

        Page inputPage;
        try (IntBlock.Builder keys = blockFactory.newIntBlockBuilder(0); LongBlock.Builder values = blockFactory.newLongBlockBuilder(0)) {
            inputPage = new Page(keys.build(), values.build());
        }
        AtomicReference<Page> pageHolder = new AtomicReference<>(inputPage);
        LocalRelation materialized = new LocalRelation(Source.EMPTY, List.of(rightKey, payload), LocalSupplier.of(inputPage));

        Project project = as(
            join.inlineSubqueryResult(materialized, resultContext(blockFactory, pageHolder, ByteSizeValue.ofMb(100))),
            Project.class
        );
        assertThat(as(project.child(), Eval.class).fields(), hasSize(1));
        assertThat(pageHolder.get(), nullValue());
    }

    public void testFinalMaterializedPageIsCheckedAgainstTheSizeLimit() {
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        ReferenceAttribute leftKey = attribute("key", DataType.INTEGER);
        ReferenceAttribute rightKey = attribute("key", DataType.INTEGER);
        ReferenceAttribute payload = attribute("payload", DataType.KEYWORD);
        SubqueryJoin join = subqueryJoin(leftKey, rightKey, payload, ExecuteLocation.ANY);

        Page inputPage;
        try (
            IntBlock.Builder keys = blockFactory.newIntBlockBuilder(1);
            BytesRefBlock.Builder values = blockFactory.newBytesRefBlockBuilder(1)
        ) {
            keys.appendInt(1);
            values.appendBytesRef(new BytesRef("payload"));
            inputPage = new Page(keys.build(), values.build());
        }
        AtomicReference<Page> pageHolder = new AtomicReference<>(inputPage);
        Block inputKeyBlock = inputPage.getBlock(0);
        Block inputPayloadBlock = inputPage.getBlock(1);
        LocalRelation materialized = new LocalRelation(Source.EMPTY, List.of(rightKey, payload), LocalSupplier.of(inputPage));

        IllegalArgumentException error = expectThrows(
            IllegalArgumentException.class,
            () -> join.inlineSubqueryResult(materialized, resultContext(blockFactory, pageHolder, ByteSizeValue.ofBytes(1)))
        );
        assertThat(error.getMessage(), containsString("sub-plan execution results too large"));
        assertThat(pageHolder.get(), nullValue());
        assertTrue(inputKeyBlock.isReleased());
        assertTrue(inputPayloadBlock.isReleased());
    }

    public void testExpandedConstantPageIsCheckedAgainstTheSizeLimit() {
        BlockFactory blockFactory = SubqueryJoinInliner.blockFactory(TestBlockFactory.getNonBreakingInstance());
        ByteSizeValue maxSize = ByteSizeValue.ofKb(128);
        ReferenceAttribute leftKey = attribute("key", DataType.INTEGER);
        ReferenceAttribute rightKey = attribute("key", DataType.INTEGER);
        ReferenceAttribute payload = attribute("payload", DataType.KEYWORD);
        SubqueryJoin join = subqueryJoin(leftKey, rightKey, payload, ExecuteLocation.ANY);
        Page compactPage = new Page(
            BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), 1, 256),
            BlockUtils.constantBlock(TestBlockFactory.getNonBreakingInstance(), "x".repeat(2048), 256)
        );
        assertTrue(compactPage.ramBytesUsedByBlocks() < maxSize.getBytes());

        Page materializedPage = new Page(SessionUtils.fromPages(List.of(rightKey, payload), List.of(compactPage), blockFactory));
        assertTrue(materializedPage.ramBytesUsedByBlocks() > maxSize.getBytes());
        AtomicReference<Page> pageHolder = new AtomicReference<>(materializedPage);
        Block inputKeyBlock = materializedPage.getBlock(0);
        try {
            LocalRelation materialized = new LocalRelation(Source.EMPTY, List.of(rightKey, payload), LocalSupplier.of(materializedPage));
            IllegalArgumentException error = expectThrows(
                IllegalArgumentException.class,
                () -> join.inlineSubqueryResult(materialized, resultContext(blockFactory, pageHolder, maxSize))
            );
            assertThat(error.getMessage(), containsString("sub-plan execution results too large"));
            assertThat(pageHolder.get(), nullValue());
            assertTrue(inputKeyBlock.isReleased());
        } finally {
            compactPage.releaseBlocks();
            Page remainingPage = pageHolder.getAndSet(null);
            if (remainingPage != null) {
                remainingPage.releaseBlocks();
            }
        }
    }

    public void testMaterializedPageUsesSerializablePrimitiveArrayBlocks() {
        BlockFactory blockFactory = SubqueryJoinInliner.blockFactory(TestBlockFactory.getNonBreakingInstance());
        ReferenceAttribute leftKey = attribute("key", DataType.INTEGER);
        ReferenceAttribute rightKey = attribute("key", DataType.INTEGER);
        ReferenceAttribute payload = attribute("payload", DataType.LONG);
        SubqueryJoin join = subqueryJoin(leftKey, rightKey, payload, ExecuteLocation.ANY);
        int positionCount = 200_000;

        Page inputPage;
        try (
            IntBlock.Builder keys = blockFactory.newIntBlockBuilder(positionCount);
            LongBlock.Builder values = blockFactory.newLongBlockBuilder(positionCount)
        ) {
            for (int p = 0; p < positionCount; p++) {
                keys.appendInt(p);
                values.appendLong(p);
            }
            inputPage = new Page(keys.build(), values.build());
        }
        AtomicReference<Page> pageHolder = new AtomicReference<>(inputPage);
        LocalRelation materialized = new LocalRelation(Source.EMPTY, List.of(rightKey, payload), LocalSupplier.of(inputPage));
        try {
            join.inlineSubqueryResult(materialized, resultContext(blockFactory, pageHolder, ByteSizeValue.ofMb(10)));
            Page shippedPage = pageHolder.get();
            assertNotNull(shippedPage);
            assertFalse(shippedPage.getBlock(0) instanceof IntBigArrayBlock);
            assertFalse(shippedPage.getBlock(1) instanceof LongBigArrayBlock);
        } finally {
            Page shippedPage = pageHolder.getAndSet(null);
            if (shippedPage != null) {
                shippedPage.releaseBlocks();
            }
        }
    }

    private static LocalRelation relation(List<Attribute> output) {
        return new LocalRelation(Source.EMPTY, output, LocalSupplier.of(new Page(0)));
    }

    private static SubqueryJoin subqueryJoin(Attribute leftKey, Attribute rightKey, Attribute payload, ExecuteLocation mode) {
        return new SubqueryJoin(
            Source.EMPTY,
            relation(List.of(leftKey)),
            relation(List.of(rightKey, payload)),
            LEFT,
            List.of(leftKey),
            List.of(rightKey),
            null,
            mode
        );
    }

    private static SubqueryResultContext resultContext(BlockFactory blockFactory, AtomicReference<Page> pageHolder, ByteSizeValue maxSize) {
        return new SubqueryResultContext(0, maxSize, blockFactory, pageHolder);
    }

    private List<String> drainWarnings() {
        List<String> warnings = threadContext.getResponseHeaders()
            .getOrDefault("Warning", List.of())
            .stream()
            .map(warning -> HeaderWarning.extractWarningValueFromWarningHeader(warning, false))
            .collect(Collectors.toCollection(ArrayList::new));
        threadContext.stashContext();
        return warnings;
    }

    private static SubqueryJoin joinForKeyTypes(DataType leftType, DataType rightType) {
        ReferenceAttribute leftKey = attribute("key", leftType);
        ReferenceAttribute rightKey = attribute("key", rightType);
        return new SubqueryJoin(
            Source.EMPTY,
            relation(List.of(leftKey)),
            relation(List.of(rightKey)),
            LEFT,
            List.of(leftKey),
            List.of(rightKey),
            null,
            ExecuteLocation.ANY
        );
    }

    private static SubqueryJoin joinWithPayload(DataType payloadType) {
        ReferenceAttribute leftKey = attribute("key", DataType.KEYWORD);
        ReferenceAttribute rightKey = attribute("key", DataType.KEYWORD);
        ReferenceAttribute payload = attribute("payload", payloadType);
        return new SubqueryJoin(
            Source.EMPTY,
            relation(List.of(leftKey)),
            relation(List.of(rightKey, payload)),
            LEFT,
            List.of(leftKey),
            List.of(rightKey),
            null,
            ExecuteLocation.ANY
        );
    }

    private static ReferenceAttribute attribute(String name, DataType dataType) {
        return new ReferenceAttribute(Source.EMPTY, null, name, dataType);
    }
}
