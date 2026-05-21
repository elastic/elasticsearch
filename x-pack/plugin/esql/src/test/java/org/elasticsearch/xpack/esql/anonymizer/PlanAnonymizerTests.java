/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.anonymizer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PlanAnonymizerTests extends ESTestCase {

    private static final String INDEX = "customer-orders-2026";
    private static final String F_EMAIL = "user_email";
    private static final String F_ORDER_TOTAL = "order_total";
    private static final String F_RETRY_COUNT = "retry_count";

    public void testRawIdentifiersAreScrubbed() {
        LogicalPlan logical = sampleLogicalPlan();
        PhysicalPlan physical = new FragmentExec(logical);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize("FROM " + INDEX, logical, physical);

        for (String secret : List.of(INDEX, F_EMAIL, F_ORDER_TOTAL, F_RETRY_COUNT, "alice@example.com")) {
            assertFalse("'" + secret + "' leaked into logical plan:\n" + out.logicalPlan(), out.logicalPlan().contains(secret));
            assertFalse("'" + secret + "' leaked into physical plan:\n" + out.physicalPlan(), out.physicalPlan().contains(secret));
            assertFalse("'" + secret + "' leaked into schema:\n" + out.schema(), out.schema().contains(secret));
        }
    }

    public void testFieldTypesPreservedInSchema() {
        LogicalPlan logical = sampleLogicalPlan();
        PhysicalPlan physical = new FragmentExec(logical);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize("FROM " + INDEX, logical, physical);

        assertTrue("schema missing 'keyword':\n" + out.schema(), out.schema().contains("keyword"));
        assertTrue("schema missing 'long':\n" + out.schema(), out.schema().contains("long"));
    }

    public void testLiteralIdentityWithinSubmission() {
        LogicalPlan logical = sampleLogicalPlan();
        PhysicalPlan physical = new FragmentExec(logical);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize("FROM " + INDEX, logical, physical);

        Matcher m = Pattern.compile("(\\d+)\\[LONG\\]").matcher(out.logicalPlan());
        Map<String, Integer> counts = new HashMap<>();
        while (m.find()) {
            counts.merge(m.group(1), 1, Integer::sum);
        }
        assertTrue(
            "expected the same LONG placeholder to appear at least twice in:\n" + out.logicalPlan() + "\ncounts=" + counts,
            counts.values().stream().anyMatch(c -> c >= 2)
        );
    }

    public void testColumnTokensStableAcrossSubmissions() {
        LogicalPlan logical = sampleLogicalPlan();
        PhysicalPlan physical = new FragmentExec(logical);
        String clusterUuid = randomUUID();

        var first = PlanAnonymizer.forSubmission(clusterUuid).anonymize("FROM " + INDEX, logical, physical);
        var second = PlanAnonymizer.forSubmission(clusterUuid).anonymize("FROM " + INDEX, logical, physical);

        assertEquals(first.logicalPlan(), second.logicalPlan());
        assertEquals(first.physicalPlan(), second.physicalPlan());
        assertEquals(first.schema(), second.schema());
    }

    public void testColumnTokensDifferAcrossClusters() {
        LogicalPlan logical = sampleLogicalPlan();
        PhysicalPlan physical = new FragmentExec(logical);

        var clusterA = PlanAnonymizer.forSubmission(randomUUID()).anonymize("FROM " + INDEX, logical, physical);
        var clusterB = PlanAnonymizer.forSubmission(randomUUID()).anonymize("FROM " + INDEX, logical, physical);

        assertNotEquals(clusterA.logicalPlan(), clusterB.logicalPlan());
        assertNotEquals(clusterA.physicalPlan(), clusterB.physicalPlan());
    }

    public void testFragmentExecInnerPlanAnonymized() {
        LogicalPlan logical = sampleLogicalPlan();
        FragmentExec physical = new FragmentExec(logical);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize("FROM " + INDEX, logical, physical);

        assertFalse("index name leaked through FragmentExec wrapper:\n" + out.physicalPlan(), out.physicalPlan().contains(INDEX));
        assertFalse("field name leaked through FragmentExec wrapper:\n" + out.physicalPlan(), out.physicalPlan().contains(F_EMAIL));
    }

    private static LogicalPlan sampleLogicalPlan() {
        EsField emailField = new EsField(F_EMAIL, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        EsField orderTotalField = new EsField(F_ORDER_TOTAL, DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        EsField retryCountField = new EsField(F_RETRY_COUNT, DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE);

        FieldAttribute email = new FieldAttribute(Source.EMPTY, null, null, F_EMAIL, emailField);
        FieldAttribute orderTotal = new FieldAttribute(Source.EMPTY, null, null, F_ORDER_TOTAL, orderTotalField);
        FieldAttribute retryCount = new FieldAttribute(Source.EMPTY, null, null, F_RETRY_COUNT, retryCountField);

        EsRelation relation = new EsRelation(
            Source.EMPTY,
            INDEX,
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(INDEX, IndexMode.STANDARD),
            List.<Attribute>of(email, orderTotal, retryCount)
        );

        Literal alice = new Literal(Source.EMPTY, new BytesRef("alice@example.com"), DataType.KEYWORD);
        Literal five = new Literal(Source.EMPTY, 5L, DataType.LONG);
        Equals emailEq = new Equals(Source.EMPTY, email, alice);
        Equals orderEq = new Equals(Source.EMPTY, orderTotal, five);
        Equals retryEq = new Equals(Source.EMPTY, retryCount, five);

        Filter filter = new Filter(Source.EMPTY, relation, new And(Source.EMPTY, emailEq, new And(Source.EMPTY, orderEq, retryEq)));
        return new Limit(Source.EMPTY, new Literal(Source.EMPTY, 100, DataType.INTEGER), filter);
    }
}
