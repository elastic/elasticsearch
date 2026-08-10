/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.anonymizer;

import org.apache.logging.log4j.Level;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class EsqlFailureLoggerTests extends ESTestCase {

    private static final String INDEX = "customer-orders-2026";
    private static final String F_EMAIL = "user_email";
    private static final String EXECUTION_DESCRIBE = """
        DriverFactory(instances = 1, type = SINGLETON)
        \\_ValuesSourceReaderOperator[fields = [user_email], index=customer-orders-2026]
        \\_EvalOperator[evaluator=LiteralsEvaluator[lit=alice@example.com]]
        \\_ExchangeSinkOperator""";

    public void testLocalComputeFailureLogsError() {
        try (var mockLog = MockLog.capture(EsqlFailureLogger.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "local compute failure",
                    EsqlFailureLogger.class.getCanonicalName(),
                    Level.ERROR,
                    "ES|QL local compute failed in session [session-1] cluster [local] shards [[idx_*][0]]*"
                        + "layout misalignment*localPhysical:*localExecution:*"
                )
            );

            EsqlFailureLogger.logLocalComputeFailure(
                localComputeContext(new ShardId(INDEX, "_na_", 0)),
                new IllegalStateException("layout misalignment")
            );
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testLocalComputeFailureDoesNotLogRawIndexNameForShards() {
        try (var mockLog = MockLog.capture(EsqlFailureLogger.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "raw index name in shard list",
                    EsqlFailureLogger.class.getCanonicalName(),
                    Level.ERROR,
                    "*" + INDEX + "*"
                )
            );
            EsqlFailureLogger.logLocalComputeFailure(
                localComputeContext(new ShardId(INDEX, "_na_", 0)),
                new IllegalStateException("layout misalignment")
            );
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testLocalComputeFailureSkips4xx() {
        try (var mockLog = MockLog.capture(EsqlFailureLogger.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no local compute failure log",
                    EsqlFailureLogger.class.getCanonicalName(),
                    Level.ERROR,
                    "ES|QL local compute failed"
                )
            );
            EsqlFailureLogger.logLocalComputeFailure(
                localComputeContext(new ShardId(INDEX, "_na_", 0)),
                new VerificationException("verification failed")
            );
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testLocalComputeFailureAnonymizesSecrets() {
        PhysicalPlan physical = new FragmentExec(sampleLogicalPlan());

        var plans = PlanAnonymizer.forSubmission(randomUUID()).anonymizeLocalCompute(physical, EXECUTION_DESCRIBE);

        for (String secret : List.of(INDEX, F_EMAIL, "alice@example.com")) {
            assertThat(plans.physical(), not(containsString(secret)));
            assertThat(plans.executionPlan(), not(containsString(secret)));
        }
        assertThat(plans.physical(), containsString("col_"));
        assertThat(plans.executionPlan(), containsString("EvalOperator"));
    }

    public void testCoordinatorFailureSkipsNullParsed() {
        try (var mockLog = MockLog.capture(EsqlFailureLogger.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no coordinator failure log",
                    EsqlFailureLogger.class.getCanonicalName(),
                    Level.ERROR,
                    "ES|QL query failed"
                )
            );
            EsqlFailureLogger.logCoordinatorFailure("session-1", randomUUID(), null, null, null, null, new IllegalStateException("boom"));
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testShouldLogInternalServerError() {
        assertTrue(EsqlFailureLogger.shouldLogInternalServerError(new IllegalStateException("boom")));
        assertFalse(EsqlFailureLogger.shouldLogInternalServerError(new VerificationException("bad query")));
    }

    private static EsqlFailureLogger.LocalComputeFailureContext localComputeContext(ShardId... shardIds) {
        return new EsqlFailureLogger.LocalComputeFailureContext(
            "session-1",
            randomUUID(),
            "local",
            List.of(shardIds),
            new FragmentExec(sampleLogicalPlan()),
            EXECUTION_DESCRIBE
        );
    }

    private static LogicalPlan sampleLogicalPlan() {
        EsField emailField = new EsField(F_EMAIL, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        FieldAttribute email = new FieldAttribute(Source.EMPTY, null, null, F_EMAIL, emailField);
        EsRelation relation = new EsRelation(
            Source.EMPTY,
            INDEX,
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(INDEX, IndexMode.STANDARD),
            List.<Attribute>of(email)
        );
        Literal alice = new Literal(Source.EMPTY, new BytesRef("alice@example.com"), DataType.KEYWORD);
        Filter filter = new Filter(Source.EMPTY, relation, new Equals(Source.EMPTY, email, alice));
        return new Limit(Source.EMPTY, new Literal(Source.EMPTY, 100, DataType.INTEGER), filter);
    }
}
