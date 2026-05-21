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

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(logical, physical);

        for (String secret : List.of(INDEX, F_EMAIL, F_ORDER_TOTAL, F_RETRY_COUNT, "alice@example.com")) {
            assertFalse("'" + secret + "' leaked into logical plan:\n" + out.logicalPlan(), out.logicalPlan().contains(secret));
            assertFalse("'" + secret + "' leaked into physical plan:\n" + out.physicalPlan(), out.physicalPlan().contains(secret));
            assertFalse("'" + secret + "' leaked into schema:\n" + out.schema(), out.schema().contains(secret));
        }
    }

    public void testFieldTypesPreservedInSchema() {
        LogicalPlan logical = sampleLogicalPlan();
        PhysicalPlan physical = new FragmentExec(logical);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(logical, physical);

        assertTrue("schema missing 'keyword':\n" + out.schema(), out.schema().contains("keyword"));
        assertTrue("schema missing 'long':\n" + out.schema(), out.schema().contains("long"));
    }

    public void testLiteralIdentityWithinSubmission() {
        LogicalPlan logical = sampleLogicalPlan();
        PhysicalPlan physical = new FragmentExec(logical);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(logical, physical);

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

        var first = PlanAnonymizer.forSubmission(clusterUuid).anonymize(logical, physical);
        var second = PlanAnonymizer.forSubmission(clusterUuid).anonymize(logical, physical);

        assertEquals(first.logicalPlan(), second.logicalPlan());
        assertEquals(first.physicalPlan(), second.physicalPlan());
        assertEquals(first.schema(), second.schema());
    }

    public void testColumnTokensDifferAcrossClusters() {
        LogicalPlan logical = sampleLogicalPlan();
        PhysicalPlan physical = new FragmentExec(logical);

        var clusterA = PlanAnonymizer.forSubmission(randomUUID()).anonymize(logical, physical);
        var clusterB = PlanAnonymizer.forSubmission(randomUUID()).anonymize(logical, physical);

        assertNotEquals(clusterA.logicalPlan(), clusterB.logicalPlan());
        assertNotEquals(clusterA.physicalPlan(), clusterB.physicalPlan());
    }

    public void testFragmentExecInnerPlanAnonymized() {
        LogicalPlan logical = sampleLogicalPlan();
        FragmentExec physical = new FragmentExec(logical);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(logical, physical);

        assertFalse("index name leaked through FragmentExec wrapper:\n" + out.physicalPlan(), out.physicalPlan().contains(INDEX));
        assertFalse("field name leaked through FragmentExec wrapper:\n" + out.physicalPlan(), out.physicalPlan().contains(F_EMAIL));
    }

    /**
     * Adversarial: build a plan stuffed with identifiers and literal values that look like real PII
     * (emails, SSNs, credit card numbers, IPs, password-like strings, dates). After anonymization no
     * input identifier — neither a field name, nor an index name, nor a string-literal value, nor a
     * numeric-literal value — must appear in any of the three artifacts. Asserted by exhaustive
     * substring scan of the rendered output against every input string we fed in.
     */
    public void testAdversarialNoPlaintextLeak() {
        List<String> sensitiveFieldNames = List.of(
            "user.email",
            "customer_ssn",
            "credit_card_number",
            "account.password_hash",
            "billing_address.zip",
            "customer.dob",
            "phone_number",
            "session_token"
        );
        List<String> sensitiveStringLiterals = List.of(
            "alice@example.com",
            "4242-4242-4242-4242",
            "555-12-3456",
            "203.0.113.42",
            "Pa$$w0rd!123",
            "2024-01-15",
            "Bearer eyJhbGciOiJIUzI1NiJ9.secret"
        );
        long sensitiveNumericLiteral = 8675309L;
        String sensitiveIndex = "prod-payments-customer-pii-2026-eu-west-1";

        List<EsField> fields = new java.util.ArrayList<>();
        List<Attribute> attrs = new java.util.ArrayList<>();
        for (String name : sensitiveFieldNames) {
            EsField f = new EsField(name, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
            fields.add(f);
            attrs.add(new FieldAttribute(Source.EMPTY, null, null, name, f));
        }
        EsField amountField = new EsField("amount", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        FieldAttribute amount = new FieldAttribute(Source.EMPTY, null, null, "amount", amountField);
        attrs.add(amount);

        EsRelation relation = new EsRelation(
            Source.EMPTY,
            sensitiveIndex,
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(sensitiveIndex, IndexMode.STANDARD),
            attrs
        );

        LogicalPlan condition = relation;
        for (int i = 0; i < sensitiveStringLiterals.size(); i++) {
            Literal lit = new Literal(Source.EMPTY, new BytesRef(sensitiveStringLiterals.get(i)), DataType.KEYWORD);
            condition = new Filter(Source.EMPTY, condition, new Equals(Source.EMPTY, attrs.get(i), lit));
        }
        Equals amountEq = new Equals(Source.EMPTY, amount, new Literal(Source.EMPTY, sensitiveNumericLiteral, DataType.LONG));
        LogicalPlan plan = new Limit(
            Source.EMPTY,
            new Literal(Source.EMPTY, 100, DataType.INTEGER),
            new Filter(Source.EMPTY, condition, amountEq)
        );
        FragmentExec physical = new FragmentExec(plan);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(plan, physical);

        List<String> mustNotAppear = new java.util.ArrayList<>();
        mustNotAppear.add(sensitiveIndex);
        mustNotAppear.addAll(sensitiveFieldNames);
        mustNotAppear.addAll(sensitiveStringLiterals);
        mustNotAppear.add(Long.toString(sensitiveNumericLiteral));

        for (String secret : mustNotAppear) {
            assertFalse("'" + secret + "' leaked into schema:\n" + out.schema(), out.schema().contains(secret));
            assertFalse("'" + secret + "' leaked into logical plan:\n" + out.logicalPlan(), out.logicalPlan().contains(secret));
            assertFalse("'" + secret + "' leaked into physical plan:\n" + out.physicalPlan(), out.physicalPlan().contains(secret));
        }
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
