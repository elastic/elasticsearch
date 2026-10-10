/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getFieldAttribute;
import static org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.LEFT;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class SubqueryLookupJoinTests extends ESTestCase {

    public void testOutputUsesRightReferencesAndPreservesLeftJoinKey() {
        FieldAttribute key = getFieldAttribute("key", DataType.INTEGER);
        FieldAttribute leftValue = getFieldAttribute("value", DataType.INTEGER);
        FieldAttribute rightValue = getFieldAttribute("value", DataType.INTEGER);
        SubqueryLookupJoin join = new SubqueryLookupJoin(
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

    public void testRebuildsRetainTheSubqueryLookupJoinType() {
        FieldAttribute key = getFieldAttribute("key", DataType.INTEGER);
        LocalRelation left = relation(List.of(key));
        LocalRelation right = relation(List.of(key));
        SubqueryLookupJoin join = new SubqueryLookupJoin(Source.EMPTY, left, right, List.of(key));

        assertThat(join.replaceChildren(left, right), instanceOf(SubqueryLookupJoin.class));
        assertThat(join.withConfig(join.config()), instanceOf(SubqueryLookupJoin.class));
    }

    public void testPostAnalysisVerificationAllowsCompatibleKeyTypes() {
        for (SubqueryLookupJoin join : List.of(
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
        for (SubqueryLookupJoin join : List.of(
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

    public void testPayloadLookupSupportCoversEveryDataType() {
        for (DataType dataType : DataType.values()) {
            boolean expected = switch (dataType) {
                case AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, TDIGEST, DATE_RANGE, DOUBLE_RANGE -> false;
                case UNSUPPORTED, NULL, BOOLEAN, COUNTER_LONG, COUNTER_INTEGER, COUNTER_DOUBLE, LONG, INTEGER, UNSIGNED_LONG, DOUBLE, SHORT,
                    BYTE, FLOAT, HALF_FLOAT, SCALED_FLOAT, KEYWORD, TEXT, DATETIME, DATE_NANOS, IP, VERSION, OBJECT, SOURCE, DATE_PERIOD,
                    TIME_DURATION, GEO_POINT, CARTESIAN_POINT, CARTESIAN_SHAPE, GEO_SHAPE, GEOHASH, GEOTILE, GEOHEX, DOC_DATA_TYPE,
                    TSID_DATA_TYPE, PARTIAL_AGG, HISTOGRAM, DENSE_VECTOR, FLATTENED -> true;
            };
            assertEquals(
                "Unexpected payload lookup support for [" + dataType + "]",
                expected,
                SubqueryLookupJoin.supportsPayloadLookup(dataType)
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
            assertThat(failures.toString(), containsString("LOOKUP JOIN subquery cannot return column [payload]"));
        }

        Failures supportedPayload = new Failures();
        joinWithPayload(DataType.LONG).postOptimizationVerification(supportedPayload);
        assertFalse(supportedPayload.toString(), supportedPayload.hasFailures());
    }

    public void testIsNotSerialized() {
        FieldAttribute key = getFieldAttribute("key", DataType.INTEGER);
        SubqueryLookupJoin join = new SubqueryLookupJoin(Source.EMPTY, relation(List.of(key)), relation(List.of(key)), List.of(key));

        expectThrows(UnsupportedOperationException.class, () -> join.writeTo((StreamOutput) null));
        expectThrows(UnsupportedOperationException.class, join::getWriteableName);
    }

    private static LocalRelation relation(List<Attribute> output) {
        return new LocalRelation(Source.EMPTY, output, LocalSupplier.of(new Page(0)));
    }

    private static SubqueryLookupJoin joinForKeyTypes(DataType leftType, DataType rightType) {
        ReferenceAttribute leftKey = attribute("key", leftType);
        ReferenceAttribute rightKey = attribute("key", rightType);
        return new SubqueryLookupJoin(
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

    private static SubqueryLookupJoin joinWithPayload(DataType payloadType) {
        ReferenceAttribute leftKey = attribute("key", DataType.KEYWORD);
        ReferenceAttribute rightKey = attribute("key", DataType.KEYWORD);
        ReferenceAttribute payload = attribute("payload", payloadType);
        return new SubqueryLookupJoin(
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
