/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.join;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn.ExecuteLocation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.List;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes.LEFT;

/**
 * A LOOKUP JOIN whose lookup rows come from an independent subquery. The coordinator materializes the right side before mapping, so this
 * node is never serialized. Right-side outputs are references because they do not belong to the left index.
 */
public class SubqueryLookupJoin extends Join implements IndependentSubqueryJoin, TelemetryAware {

    public SubqueryLookupJoin(Source source, LogicalPlan left, LogicalPlan subquery, List<Attribute> joinFields) {
        this(source, left, subquery, new JoinConfig(LEFT, joinFields, joinFields, null), ExecuteLocation.ANY);
    }

    public SubqueryLookupJoin(Source source, LogicalPlan left, LogicalPlan subquery, JoinConfig config, ExecuteLocation mode) {
        super(source, left, subquery, config, mode);
    }

    public SubqueryLookupJoin(
        Source source,
        LogicalPlan left,
        LogicalPlan subquery,
        JoinType type,
        List<Attribute> leftFields,
        List<Attribute> rightFields,
        Expression joinOnConditions,
        ExecuteLocation mode
    ) {
        this(source, left, subquery, new JoinConfig(type, leftFields, rightFields, joinOnConditions), mode);
    }

    @Override
    public List<NamedExpression> computeOutputExpressions(List<? extends NamedExpression> left, List<? extends NamedExpression> right) {
        return Join.computeOutputExpressions(left, makeReference(Expressions.asAttributes(right)), config());
    }

    @Override
    public SubqueryLookupJoin replaceChildren(LogicalPlan left, LogicalPlan right) {
        return new SubqueryLookupJoin(source(), left, right, config(), executesOn());
    }

    @Override
    public SubqueryLookupJoin withConfig(JoinConfig config) {
        return new SubqueryLookupJoin(source(), left(), right(), config, executesOn());
    }

    @Override
    protected NodeInfo<Join> info() {
        return NodeInfo.create(
            this,
            SubqueryLookupJoin::new,
            left(),
            right(),
            config().type(),
            config().leftFields(),
            config().rightFields(),
            config().joinOnConditions(),
            executesOn()
        );
    }

    @Override
    public void writeTo(StreamOutput out) {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public String telemetryLabel() {
        return "LOOKUP JOIN SUBQUERY";
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        for (int i = 0; i < config().leftFields().size(); i++) {
            Attribute leftField = config().leftFields().get(i);
            Attribute rightField = config().rightFields().get(i);
            if (Join.comparableTypes(leftField, rightField) == false) {
                failures.add(
                    fail(
                        leftField,
                        "LOOKUP JOIN subquery left field [{}] of type [{}] is incompatible with right field [{}] of type [{}]",
                        leftField.name(),
                        leftField.dataType(),
                        rightField.name(),
                        rightField.dataType()
                    )
                );
            }
            if (AbstractSubqueryJoin.isSubqueryJoinUnsupported(rightField.dataType())) {
                failures.add(
                    fail(
                        leftField,
                        "LOOKUP JOIN subquery with right field [{}] of type [{}] is not supported",
                        rightField.name(),
                        rightField.dataType()
                    )
                );
            }
        }
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        for (Attribute rightField : rightOutputFields()) {
            if (supportsPayloadLookup(rightField.dataType()) == false) {
                failures.add(
                    fail(
                        rightField,
                        "LOOKUP JOIN subquery cannot return column [{}] of type [{}]; remove it with KEEP or DROP",
                        rightField.name(),
                        rightField.dataType()
                    )
                );
            }
        }
    }

    static boolean supportsPayloadLookup(DataType dataType) {
        return switch (dataType) {
            case AGGREGATE_METRIC_DOUBLE, EXPONENTIAL_HISTOGRAM, TDIGEST, DATE_RANGE, DOUBLE_RANGE -> false;
            case UNSUPPORTED, NULL, BOOLEAN, COUNTER_LONG, COUNTER_INTEGER, COUNTER_DOUBLE, LONG, INTEGER, UNSIGNED_LONG, DOUBLE, SHORT,
                BYTE, FLOAT, HALF_FLOAT, SCALED_FLOAT, KEYWORD, TEXT, DATETIME, DATE_NANOS, IP, VERSION, OBJECT, SOURCE, DATE_PERIOD,
                TIME_DURATION, GEO_POINT, CARTESIAN_POINT, CARTESIAN_SHAPE, GEO_SHAPE, GEOHASH, GEOTILE, GEOHEX, DOC_DATA_TYPE,
                TSID_DATA_TYPE, PARTIAL_AGG, HISTOGRAM, DENSE_VECTOR, FLATTENED -> true;
        };
    }

    @Override
    public LogicalPlan inlineSubqueryResult(LocalRelation result, SubqueryResultContext context) {
        return SubqueryLookupJoinInliner.inline(this, result, context);
    }
}
