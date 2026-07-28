/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.anonymizer;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.dissect.DissectParser;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.anonymizer.AnonymizationContext;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.RLikePatternList;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPatternList;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.RLikeList;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLikeList;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plan.logical.UriParts;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatcher;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LabelMatchers;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.UriPartsExec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, logical, physical);

        for (String secret : List.of(INDEX, F_EMAIL, F_ORDER_TOTAL, F_RETRY_COUNT, "alice@example.com")) {
            assertFalse("'" + secret + "' leaked into logical plan:\n" + out.optimized(), out.optimized().contains(secret));
            assertFalse("'" + secret + "' leaked into physical plan:\n" + out.physical(), out.physical().contains(secret));
            assertFalse("'" + secret + "' leaked into schema:\n" + out.schema(), out.schema().contains(secret));
        }
    }

    public void testFieldTypesPreservedInSchema() {
        LogicalPlan logical = sampleLogicalPlan();
        PhysicalPlan physical = new FragmentExec(logical);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, logical, physical);

        assertTrue("schema missing 'keyword':\n" + out.schema(), out.schema().contains("keyword"));
        assertTrue("schema missing 'long':\n" + out.schema(), out.schema().contains("long"));
    }

    public void testLiteralIdentityWithinSubmission() {
        LogicalPlan logical = sampleLogicalPlan();
        PhysicalPlan physical = new FragmentExec(logical);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, logical, physical);

        Matcher m = Pattern.compile("(\\d+)\\[LONG\\]").matcher(out.optimized());
        Map<String, Integer> counts = new HashMap<>();
        while (m.find()) {
            counts.merge(m.group(1), 1, Integer::sum);
        }
        assertTrue(
            "expected the same LONG placeholder to appear at least twice in:\n" + out.optimized() + "\ncounts=" + counts,
            counts.values().stream().anyMatch(c -> c >= 2)
        );
    }

    public void testColumnTokensStableAcrossSubmissions() {
        LogicalPlan logical = sampleLogicalPlan();
        PhysicalPlan physical = new FragmentExec(logical);
        String clusterUuid = randomUUID();

        var first = PlanAnonymizer.forSubmission(clusterUuid).anonymize(null, null, logical, physical);
        var second = PlanAnonymizer.forSubmission(clusterUuid).anonymize(null, null, logical, physical);

        assertEquals(first.optimized(), second.optimized());
        assertEquals(first.physical(), second.physical());
        assertEquals(first.schema(), second.schema());
    }

    public void testColumnTokensDifferAcrossClusters() {
        LogicalPlan logical = sampleLogicalPlan();
        PhysicalPlan physical = new FragmentExec(logical);

        var clusterA = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, logical, physical);
        var clusterB = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, logical, physical);

        assertNotEquals(clusterA.optimized(), clusterB.optimized());
        assertNotEquals(clusterA.physical(), clusterB.physical());
    }

    public void testFragmentExecInnerPlanAnonymized() {
        LogicalPlan logical = sampleLogicalPlan();
        FragmentExec physical = new FragmentExec(logical);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, logical, physical);

        assertFalse("index name leaked through FragmentExec wrapper:\n" + out.physical(), out.physical().contains(INDEX));
        assertFalse("field name leaked through FragmentExec wrapper:\n" + out.physical(), out.physical().contains(F_EMAIL));
    }

    /**
     * NamedSubquery carries the view name a sub-plan was resolved from; its nodeString surfaces
     * the name as {@code NamedSubquery[<view>]}. Verifies the rule anonymizes it.
     */
    public void testNamedSubqueryNameAnonymized() {
        String sensitiveView = "internal_users_v2_2026q1";
        EsField field = new EsField(F_EMAIL, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        FieldAttribute attr = new FieldAttribute(Source.EMPTY, null, null, F_EMAIL, field);
        EsRelation inner = new EsRelation(
            Source.EMPTY,
            INDEX,
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(INDEX, IndexMode.STANDARD),
            List.<Attribute>of(attr)
        );
        org.elasticsearch.xpack.esql.plan.logical.NamedSubquery ns = new org.elasticsearch.xpack.esql.plan.logical.NamedSubquery(
            Source.EMPTY,
            inner,
            sensitiveView
        );
        LogicalPlan plan = new Limit(Source.EMPTY, new Literal(Source.EMPTY, 10, DataType.INTEGER), ns);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(plan, null, null, null);

        assertFalse("view name leaked into parsed plan:\n" + out.parsed(), out.parsed().contains(sensitiveView));
    }

    /**
     * ViewUnionAll holds a {@code LinkedHashMap<viewName, subPlan>} that surfaces in its nodeString
     * as {@code ViewUnionAll[[view1, view2]]}. Verifies every key in the map is anonymized.
     */
    public void testViewUnionAllNamesAnonymized() {
        List<String> sensitiveViews = List.of("payments_v1", "tenant_pii_eu", "billing_secrets_v3");
        EsField field = new EsField(F_EMAIL, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        FieldAttribute attr = new FieldAttribute(Source.EMPTY, null, null, F_EMAIL, field);
        EsRelation inner = new EsRelation(
            Source.EMPTY,
            INDEX,
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(INDEX, IndexMode.STANDARD),
            List.<Attribute>of(attr)
        );
        java.util.LinkedHashMap<String, LogicalPlan> namedSubqueries = new java.util.LinkedHashMap<>();
        for (String v : sensitiveViews) {
            namedSubqueries.put(v, inner);
        }
        org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll vua = new org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll(
            Source.EMPTY,
            namedSubqueries,
            List.<Attribute>of(attr)
        );
        LogicalPlan plan = new Limit(Source.EMPTY, new Literal(Source.EMPTY, 10, DataType.INTEGER), vua);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(plan, null, null, null);

        for (String v : sensitiveViews) {
            assertFalse("view name '" + v + "' leaked into parsed plan:\n" + out.parsed(), out.parsed().contains(v));
        }
    }

    /**
     * Multifield case: {@code job} is a text field with a {@code job.raw} keyword sub-field. The
     * sub-field name is held in {@code EsField.properties}. Verifies the recursive sub-field
     * property anonymization picks it up so neither the parent name nor the sub-field name leaks.
     */
    public void testMultifieldSubfieldPropertyAnonymized() {
        String sensitiveParent = "tenant_secret_blob";
        String sensitiveSubfield = "tenant_secret_blob_indexed";
        EsField rawField = new EsField(sensitiveSubfield, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        EsField textField = new EsField(sensitiveParent, DataType.TEXT, Map.of("raw", rawField), false, EsField.TimeSeriesFieldType.NONE);
        FieldAttribute attr = new FieldAttribute(Source.EMPTY, null, null, sensitiveParent, textField);
        EsRelation rel = new EsRelation(
            Source.EMPTY,
            INDEX,
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(INDEX, IndexMode.STANDARD),
            List.<Attribute>of(attr)
        );
        LogicalPlan plan = new Limit(Source.EMPTY, new Literal(Source.EMPTY, 10, DataType.INTEGER), rel);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, plan, null);

        assertFalse("parent field name leaked into optimized:\n" + out.optimized(), out.optimized().contains(sensitiveParent));
        assertFalse("sub-field name leaked into schema:\n" + out.schema(), out.schema().contains(sensitiveSubfield));
        assertFalse("parent field name leaked into schema:\n" + out.schema(), out.schema().contains(sensitiveParent));
    }

    /**
     * EsField subclass reconstruction: after FieldAttribute anonymization the underlying field is a
     * rebuilt subclass instance that preserves the type-specific state. For KeywordEsField that's
     * {@code precision} and {@code normalized}; the schema artifact should still surface those
     * flags via its renderer reading the original (pre-anonymization) plan.
     */
    public void testKeywordEsFieldSubclassFlagsPreservedInSchema() {
        String fieldName = "secret_tag";
        EsField keyword = new org.elasticsearch.xpack.esql.core.type.KeywordEsField(
            fieldName,
            Map.of(),
            true,
            128,
            true,
            false,
            EsField.TimeSeriesFieldType.NONE
        );
        FieldAttribute attr = new FieldAttribute(Source.EMPTY, null, null, fieldName, keyword);
        EsRelation rel = new EsRelation(
            Source.EMPTY,
            INDEX,
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(INDEX, IndexMode.STANDARD),
            List.<Attribute>of(attr)
        );

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, rel, null);

        assertFalse("field name leaked:\n" + out.schema(), out.schema().contains(fieldName));
        assertTrue("KeywordEsField subclass not surfaced:\n" + out.schema(), out.schema().contains("kind=KeywordEsField"));
        assertTrue("ignore_above not surfaced:\n" + out.schema(), out.schema().contains("ignore_above=128"));
        assertTrue("normalized flag not surfaced:\n" + out.schema(), out.schema().contains("normalized"));
    }

    /**
     * Defense-in-depth: {@code MultiTypeEsField.indexToConversionExpressions} holds {@code FieldAttribute}s
     * with their original (pre-anonymization) names inside the EsField. The plan-tree expression
     * walker doesn't descend into these because they live in field-internal state, not as plan
     * children. The anonymizer's subclass-aware reconstruction falls back to a base {@code EsField}
     * for {@code MultiTypeEsField}, dropping the conversions entirely; this test asserts the
     * post-anonymization artifacts carry no trace of the original FieldAttribute names that were
     * referenced inside those conversions.
     */
    public void testMultiTypeEsFieldConversionsDoNotLeak() {
        String sensitiveInnerName = "internal_user_secret_v1";
        EsField innerKeyword = new EsField(sensitiveInnerName, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        FieldAttribute innerAttr = new FieldAttribute(Source.EMPTY, null, null, sensitiveInnerName, innerKeyword);

        Map<String, org.elasticsearch.xpack.esql.core.expression.Expression> conversions = Map.of(
            "idx_alpha",
            innerAttr,
            "idx_beta",
            innerAttr
        );
        org.elasticsearch.xpack.esql.core.type.MultiTypeEsField mtf = new org.elasticsearch.xpack.esql.core.type.MultiTypeEsField(
            "outer_conflict",
            DataType.LONG,
            true,
            conversions,
            EsField.TimeSeriesFieldType.NONE,
            null
        );
        FieldAttribute outer = new FieldAttribute(Source.EMPTY, null, null, "outer_conflict", mtf);
        EsRelation rel = new EsRelation(
            Source.EMPTY,
            INDEX,
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(INDEX, IndexMode.STANDARD),
            List.<Attribute>of(outer)
        );
        LogicalPlan plan = new Limit(Source.EMPTY, new Literal(Source.EMPTY, 10, DataType.INTEGER), rel);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, plan, null);

        assertFalse(
            "inner FieldAttribute name in MultiTypeEsField conversions leaked into optimized:\n" + out.optimized(),
            out.optimized().contains(sensitiveInnerName)
        );
        assertFalse("inner FieldAttribute name leaked into schema:\n" + out.schema(), out.schema().contains(sensitiveInnerName));
    }

    /**
     * Defense-in-depth: {@code InvalidMappedField.typesToIndices} holds a map of conflicting types
     * to the set of index names that carried that type. The plan-tree walker doesn't descend into
     * it. The subclass-aware reconstruction falls back to a base {@code EsField}, dropping the
     * map; schema render emits only the type set, never the indices. This test asserts no original
     * index name from that map survives in the anonymized artifacts.
     */
    public void testInvalidMappedFieldIndicesDoNotLeak() {
        String sensitiveIdxA = "prod-secrets-bucket-alpha-eu";
        String sensitiveIdxB = "prod-secrets-bucket-beta-us";
        java.util.Map<String, java.util.Set<String>> typesToIndices = new java.util.HashMap<>();
        typesToIndices.put("keyword", java.util.Set.of(sensitiveIdxA));
        typesToIndices.put("long", java.util.Set.of(sensitiveIdxB));
        org.elasticsearch.xpack.esql.core.type.InvalidMappedField imf = new org.elasticsearch.xpack.esql.core.type.InvalidMappedField(
            "conflict_field",
            typesToIndices
        );
        FieldAttribute attr = new FieldAttribute(Source.EMPTY, null, null, "conflict_field", imf);
        EsRelation rel = new EsRelation(
            Source.EMPTY,
            INDEX,
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(INDEX, IndexMode.STANDARD),
            List.<Attribute>of(attr)
        );
        LogicalPlan plan = new Limit(Source.EMPTY, new Literal(Source.EMPTY, 10, DataType.INTEGER), rel);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, plan, null);

        for (String idx : List.of(sensitiveIdxA, sensitiveIdxB)) {
            assertFalse(
                "index name '" + idx + "' from InvalidMappedField.typesToIndices leaked into optimized:\n" + out.optimized(),
                out.optimized().contains(idx)
            );
            assertFalse(
                "index name '" + idx + "' from InvalidMappedField.typesToIndices leaked into schema:\n" + out.schema(),
                out.schema().contains(idx)
            );
        }
    }

    /**
     * Alias.name from EVAL/STATS (e.g. {@code EVAL bonus = ...}) is rendered by Alias.nodeString as
     * {@code ... AS bonus#NN}. Alias is NamedExpression, not Attribute, so the Attribute rule
     * never touched it before. Verifies the dedicated Alias rule scrubs it.
     */
    public void testEvalAliasNameAnonymized() {
        String sensitiveAlias = "user_secret_bonus";
        EsField salaryField = new EsField("salary", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        FieldAttribute salary = new FieldAttribute(Source.EMPTY, null, null, "salary", salaryField);
        EsRelation rel = new EsRelation(
            Source.EMPTY,
            INDEX,
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(INDEX, IndexMode.STANDARD),
            List.<Attribute>of(salary)
        );
        org.elasticsearch.xpack.esql.core.expression.Alias alias = new org.elasticsearch.xpack.esql.core.expression.Alias(
            Source.EMPTY,
            sensitiveAlias,
            salary
        );
        org.elasticsearch.xpack.esql.plan.logical.Eval eval = new org.elasticsearch.xpack.esql.plan.logical.Eval(
            Source.EMPTY,
            rel,
            List.of(alias)
        );
        LogicalPlan plan = new Limit(Source.EMPTY, new Literal(Source.EMPTY, 10, DataType.INTEGER), eval);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, plan, null);

        assertFalse("EVAL alias name leaked into optimized plan:\n" + out.optimized(), out.optimized().contains(sensitiveAlias));
    }

    /**
     * Enrich.concreteIndices holds a {@code Map<clusterAlias, enrichIndexName>} that renders via
     * NodeInfo args. Both keys and values are anonymized via the index-token map.
     */
    public void testEnrichConcreteIndicesAnonymized() {
        String sensitiveCluster = "internal_eu_west_billing";
        String sensitiveEnrichIdx = ".enrich-customer-pii-by-id-2026";
        EsField field = new EsField("customer_id", DataType.LONG, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        FieldAttribute attr = new FieldAttribute(Source.EMPTY, null, null, "customer_id", field);
        EsRelation rel = new EsRelation(
            Source.EMPTY,
            INDEX,
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(INDEX, IndexMode.STANDARD),
            List.<Attribute>of(attr)
        );
        org.elasticsearch.xpack.esql.plan.logical.Enrich enrich = new org.elasticsearch.xpack.esql.plan.logical.Enrich(
            Source.EMPTY,
            rel,
            org.elasticsearch.xpack.esql.plan.logical.Enrich.Mode.ANY,
            new Literal(Source.EMPTY, new BytesRef("departments"), DataType.KEYWORD),
            attr,
            null,
            Map.of(sensitiveCluster, sensitiveEnrichIdx),
            List.of()
        );

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, enrich, null);

        assertFalse("Enrich.concreteIndices cluster key leaked:\n" + out.optimized(), out.optimized().contains(sensitiveCluster));
        assertFalse("Enrich.concreteIndices enrich-index value leaked:\n" + out.optimized(), out.optimized().contains(sensitiveEnrichIdx));
    }

    /**
     * Dissect renders its pattern via the {@code Dissect.Parser} {@link
     * org.elasticsearch.xpack.esql.core.tree.NodeStringRenderable}. The capture names embedded in the
     * pattern and the {@code APPEND_SEPARATOR} are user-supplied and must route through the column
     * map; the {@code %{...}} structure stays. Pins the full-node anonymized render — previously only
     * the {@code rewriteDissectPattern} helper was covered in isolation, never the node end-to-end.
     */
    public void testDissectPatternCaptureAndSeparatorAnonymized() {
        String captureEmail = "user_secret_email";
        String captureSsn = "customer_ssn_capture";
        String appendSeparator = "SENSITIVE_SEPARATOR";
        String pattern = "%{" + captureEmail + "} - %{" + captureSsn + "}";

        EsField field = new EsField("message", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        FieldAttribute input = new FieldAttribute(Source.EMPTY, null, null, "message", field);
        EsRelation rel = new EsRelation(
            Source.EMPTY,
            INDEX,
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(INDEX, IndexMode.STANDARD),
            List.<Attribute>of(input)
        );
        Dissect.Parser parser = new Dissect.Parser(pattern, appendSeparator, new DissectParser(pattern, appendSeparator));
        Dissect dissect = new Dissect(Source.EMPTY, rel, input, parser, parser.keyAttributes(Source.EMPTY));

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, dissect, null);

        for (String secret : List.of(captureEmail, captureSsn, appendSeparator)) {
            assertFalse("Dissect leaked '" + secret + "':\n" + out.optimized(), out.optimized().contains(secret));
        }
        // The structural marker survives so the shape is still readable.
        assertTrue("expected %{...} structure preserved:\n" + out.optimized(), out.optimized().contains("%{"));
    }

    /**
     * Grok renders its pattern via the {@code Grok.Parser} {@link
     * org.elasticsearch.xpack.esql.core.tree.NodeStringRenderable}. The Grok library identifier (IP,
     * WORD, ...) is not customer data and passes through; the capture name after the colon is and must
     * route through the column map. Pins the full-node anonymized render end-to-end.
     */
    public void testGrokPatternCaptureAnonymized() {
        String capture = "client_secret_ip";
        String pattern = "%{IP:" + capture + "}";

        EsField field = new EsField("message", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        FieldAttribute input = new FieldAttribute(Source.EMPTY, null, null, "message", field);
        EsRelation rel = new EsRelation(
            Source.EMPTY,
            INDEX,
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(INDEX, IndexMode.STANDARD),
            List.<Attribute>of(input)
        );
        Grok.Parser parser = Grok.pattern(Source.EMPTY, pattern);
        Grok grok = new Grok(Source.EMPTY, rel, input, parser, parser.extractedFields());

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, grok, null);

        assertFalse("Grok leaked capture name '" + capture + "':\n" + out.optimized(), out.optimized().contains(capture));
        assertTrue("expected Grok library id IP preserved:\n" + out.optimized(), out.optimized().contains("%{IP:"));
    }

    private FieldAttribute keywordAttr(String name) {
        return new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            name,
            new EsField(name, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
    }

    private EsRelation singleAttrRelation(Attribute attr) {
        return new EsRelation(
            Source.EMPTY,
            INDEX,
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(INDEX, IndexMode.STANDARD),
            List.of(attr)
        );
    }

    /**
     * A spatial literal (geo_point/geo_shape/cartesian_*) carries WKB binary, not UTF-8. The literal
     * keying path used to call {@code BytesRef.utf8ToString()} on it, which garbles or throws.
     * Anonymizing a plan with a spatial literal must complete and tokenize the value, not blow up.
     */
    public void testSpatialLiteralAnonymizedDoesNotThrowOrLeak() {
        // Truncated multi-byte sequence: invalid UTF-8 that the old utf8ToString path would mangle.
        BytesRef wkb = new BytesRef(new byte[] { (byte) 0xF0, (byte) 0x28, (byte) 0x8C, (byte) 0x28 });
        FieldAttribute attr = new FieldAttribute(
            Source.EMPTY,
            null,
            null,
            "location",
            new EsField("location", DataType.GEO_POINT, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        Literal geoLiteral = new Literal(Source.EMPTY, wkb, DataType.GEO_POINT);
        LogicalPlan plan = new Filter(Source.EMPTY, singleAttrRelation(attr), new Equals(Source.EMPTY, attr, geoLiteral));

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, plan, null);

        assertNotNull("anonymization must complete for a spatial literal", out.optimized());
        assertTrue("expected the literal type to render:\n" + out.optimized(), out.optimized().contains("[GEO_POINT]"));
    }

    /**
     * LIKE with a list of patterns (`field LIKE ("a*", "b*", "c*")`) must sanitize EVERY pattern, not
     * just the first — the literal portions of all of them route through the mapper.
     */
    public void testWildcardLikeListAllPatternsAnonymized() {
        String s1 = "secret_alpha";
        String s2 = "secret_bravo";
        String s3 = "secret_charlie";
        FieldAttribute attr = keywordAttr("host");
        WildcardPatternList patterns = new WildcardPatternList(
            List.of(new WildcardPattern(s1 + "*"), new WildcardPattern(s2 + "*"), new WildcardPattern(s3 + "*"))
        );
        WildcardLikeList like = new WildcardLikeList(Source.EMPTY, attr, patterns, false);
        LogicalPlan plan = new Filter(Source.EMPTY, singleAttrRelation(attr), like);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, plan, null);

        for (String secret : List.of(s1, s2, s3)) {
            assertFalse("LIKE-list leaked '" + secret + "':\n" + out.optimized(), out.optimized().contains(secret));
        }
    }

    /**
     * RLIKE carries a regex whose literal portions are user content (here an email). Those literals
     * must not survive anonymization, even though the surrounding regex metacharacters are structural.
     */
    public void testRlikeRegexLiteralAnonymized() {
        String secretEmail = "customer@secret.example";
        FieldAttribute attr = keywordAttr("email");
        RLike rlike = new RLike(Source.EMPTY, attr, new RLikePattern(".*" + secretEmail + ".*"), false);
        LogicalPlan plan = new Filter(Source.EMPTY, singleAttrRelation(attr), rlike);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, plan, null);

        assertFalse("RLIKE leaked regex literal '" + secretEmail + "':\n" + out.optimized(), out.optimized().contains(secretEmail));
    }

    /**
     * PROMQL label matchers carry a label name and a (potentially sensitive) match value. Both must
     * tokenize through the mapper; the match operator is structural and stays. Identity rendering is
     * byte-identical to {@code toString()}.
     */
    public void testPromqlLabelMatchersAnonymized() {
        var ctx = AnonymizationContext.forSubmission(randomUUID());
        String labelName = "pod";
        String eqValue = "customer-secret-pod-123";
        String regValue = "customer-secret-5..";
        String nregValue = "internal-prod-.*";
        // Cover EQ plus the regex matchers (REG / NREG) — all route the value through mapper.column.
        LabelMatchers matchers = new LabelMatchers(
            List.of(
                new LabelMatcher(labelName, eqValue, LabelMatcher.Matcher.EQ),
                new LabelMatcher("status", regValue, LabelMatcher.Matcher.REG),
                new LabelMatcher("env", nregValue, LabelMatcher.Matcher.NREG)
            )
        );

        StringBuilder anon = new StringBuilder();
        matchers.nodeString(anon, Node.NodeStringFormat.LIMITED, ctx.mapper());
        for (String secret : List.of(labelName, eqValue, regValue, nregValue, "status", "env")) {
            assertFalse("PROMQL label content leaked '" + secret + "': " + anon, anon.toString().contains(secret));
        }

        StringBuilder identity = new StringBuilder();
        matchers.nodeString(identity, Node.NodeStringFormat.LIMITED, NodeStringMapper.IDENTITY);
        assertEquals("identity rendering must be byte-identical to toString()", matchers.toString(), identity.toString());
    }

    /**
     * EsRelation renders the resolved concrete indices only when resolution changed the pattern
     * (a wildcard / alias expanding to different concrete names) — not for an explicit list that
     * resolves to itself. Verifies: the suppressed case adds no block; the informative case renders
     * each concrete index as an {@code idx_} token under anonymization and shows the block (in both
     * modes) under identity.
     */
    public void testEsRelationResolvedIndicesShownWhenInformative() {
        FieldAttribute attr = keywordAttr("message");

        // Suppressed: explicit FROM employees resolving to exactly {employees=STANDARD}.
        EsRelation echoed = new EsRelation(
            Source.EMPTY,
            "employees",
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of("employees", IndexMode.STANDARD),
            List.of(attr)
        );
        assertFalse(
            "redundant resolved block must be suppressed: " + echoed.nodeString(),
            echoed.nodeString().contains("employees=STANDARD")
        );

        // Informative: pattern logs-* resolving to two distinct concrete indices.
        String pattern = "logs-*";
        String c1 = "logs-2026-01";
        String c2 = "logs-2026-02";
        java.util.LinkedHashMap<String, IndexMode> resolved = new java.util.LinkedHashMap<>();
        resolved.put(c1, IndexMode.STANDARD);
        resolved.put(c2, IndexMode.STANDARD);
        EsRelation expanded = new EsRelation(Source.EMPTY, pattern, IndexMode.STANDARD, Map.of(), Map.of(), resolved, List.of(attr));

        assertTrue("identity must show the resolved block", expanded.nodeString().contains("[" + c1 + "=STANDARD, " + c2 + "=STANDARD]"));

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, expanded, null);
        for (String secret : List.of(pattern, c1, c2)) {
            assertFalse("EsRelation leaked '" + secret + "':\n" + out.optimized(), out.optimized().contains(secret));
        }
    }

    /**
     * LIKE / RLIKE pattern lists render every element through the mapper and, for multiple patterns,
     * use the parenthesized {@code (p1, p2)} shape (the legacy outer-quote-wrapped form was dropped
     * in the renderable refactor). Pins both the identity shape and that every element is scrubbed.
     */
    public void testPatternListsRenderEveryElementAndShape() {
        FieldAttribute attr = keywordAttr("host");

        WildcardLikeList like = new WildcardLikeList(
            Source.EMPTY,
            attr,
            new WildcardPatternList(List.of(new WildcardPattern("secret_a*"), new WildcardPattern("secret_b*"))),
            false
        );
        String likeStr = like.nodeString();
        assertTrue("multi-pattern LIKE uses parenthesized list shape: " + likeStr, likeStr.contains("(\"secret_a*\", \"secret_b*\")"));
        assertFalse("legacy outer-quote-wrapped list form should be gone: " + likeStr, likeStr.contains("\"(\""));

        RLikeList rlike = new RLikeList(
            Source.EMPTY,
            attr,
            new RLikePatternList(List.of(new RLikePattern("secret_c.*"), new RLikePattern("secret_d.*"))),
            false
        );
        var out = PlanAnonymizer.forSubmission(randomUUID())
            .anonymize(null, null, new Filter(Source.EMPTY, singleAttrRelation(attr), rlike), null);
        for (String secret : List.of("secret_c", "secret_d")) {
            assertFalse("RLIKE-list leaked '" + secret + "':\n" + out.optimized(), out.optimized().contains(secret));
        }
    }

    /**
     * CompoundOutputEval / CompoundOutputEvalExec (URI_PARTS, USER_AGENT, REGISTERED_DOMAIN) carry a
     * raw {@code List<String> outputFieldNames} alongside their (already-anonymized) output
     * attributes. Without a {@code nodeString} override that list renders unmapped through the
     * default property walker. Assert the names route through the column-token map in both the
     * logical and physical stages.
     */
    public void testCompoundOutputFieldNamesAnonymized() {
        String sensitiveOutputName = "secret_output_capture_name";
        String sensitiveInputCol = "secret_source_url_column";

        EsField urlField = new EsField(sensitiveInputCol, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        FieldAttribute urlAttr = new FieldAttribute(Source.EMPTY, null, null, sensitiveInputCol, urlField);
        EsRelation rel = new EsRelation(
            Source.EMPTY,
            INDEX,
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(INDEX, IndexMode.STANDARD),
            List.<Attribute>of(urlAttr)
        );
        EsField outField = new EsField(sensitiveOutputName, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        FieldAttribute outAttr = new FieldAttribute(Source.EMPTY, null, null, sensitiveOutputName, outField);

        UriParts logical = new UriParts(Source.EMPTY, rel, urlAttr, List.of(sensitiveOutputName), List.<Attribute>of(outAttr));
        UriPartsExec physical = new UriPartsExec(
            Source.EMPTY,
            new FragmentExec(rel),
            urlAttr,
            List.of(sensitiveOutputName),
            List.<Attribute>of(outAttr)
        );

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, logical, physical);

        assertFalse("output field name leaked in logical plan:\n" + out.optimized(), out.optimized().contains(sensitiveOutputName));
        assertFalse("output field name leaked in physical plan:\n" + out.physical(), out.physical().contains(sensitiveOutputName));
        assertFalse("input column leaked in logical plan:\n" + out.optimized(), out.optimized().contains(sensitiveInputCol));
        assertFalse("input column leaked in physical plan:\n" + out.physical(), out.physical().contains(sensitiveInputCol));
    }

    /**
     * FragmentExec.esFilter is the DSL passthrough from {@code request.filter()} — opaque content
     * we can't safely parse. Anonymization nulls it out so no raw DSL text reaches the log.
     */
    public void testFragmentExecEsFilterDropped() {
        String sensitiveTerm = "customer_account_12345";
        org.elasticsearch.index.query.QueryBuilder filter = new org.elasticsearch.index.query.TermQueryBuilder("account_id", sensitiveTerm);
        LogicalPlan inner = sampleLogicalPlan();
        org.elasticsearch.xpack.esql.plan.physical.FragmentExec frag = new org.elasticsearch.xpack.esql.plan.physical.FragmentExec(
            Source.EMPTY,
            inner,
            filter,
            0
        );

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, inner, frag);

        assertFalse("DSL filter content leaked into physical plan:\n" + out.physical(), out.physical().contains(sensitiveTerm));
        assertFalse("DSL field name leaked:\n" + out.physical(), out.physical().contains("account_id"));
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
        String dissectCapture = "leaked_dissect_capture_name";
        String grokCapture = "leaked_grok_capture_name";

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
        // Walk DISSECT + GROK too: their capture names are user-supplied and must not survive raw.
        String dissectPattern = "%{" + dissectCapture + "}";
        Dissect.Parser dParser = new Dissect.Parser(dissectPattern, "", new DissectParser(dissectPattern, ""));
        condition = new Dissect(Source.EMPTY, condition, attrs.get(0), dParser, dParser.keyAttributes(Source.EMPTY));
        String grokPattern = "%{WORD:" + grokCapture + "}";
        Grok.Parser gParser = Grok.pattern(Source.EMPTY, grokPattern);
        condition = new Grok(Source.EMPTY, condition, attrs.get(0), gParser, gParser.extractedFields());

        Equals amountEq = new Equals(Source.EMPTY, amount, new Literal(Source.EMPTY, sensitiveNumericLiteral, DataType.LONG));
        LogicalPlan plan = new Limit(
            Source.EMPTY,
            new Literal(Source.EMPTY, 100, DataType.INTEGER),
            new Filter(Source.EMPTY, condition, amountEq)
        );
        FragmentExec physical = new FragmentExec(plan);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, plan, physical);

        List<String> mustNotAppear = new java.util.ArrayList<>();
        mustNotAppear.add(sensitiveIndex);
        mustNotAppear.addAll(sensitiveFieldNames);
        mustNotAppear.addAll(sensitiveStringLiterals);
        mustNotAppear.add(Long.toString(sensitiveNumericLiteral));
        mustNotAppear.add(dissectCapture);
        mustNotAppear.add(grokCapture);

        for (String secret : mustNotAppear) {
            assertFalse("'" + secret + "' leaked into schema:\n" + out.schema(), out.schema().contains(secret));
            assertFalse("'" + secret + "' leaked into logical plan:\n" + out.optimized(), out.optimized().contains(secret));
            assertFalse("'" + secret + "' leaked into physical plan:\n" + out.physical(), out.physical().contains(secret));
        }
    }

    /**
     * Synthetic union-type attributes carry a separate parentName that flows through
     * {@link FieldAttribute#fieldName()} — if the anonymizer only rewrites the display name via
     * {@code withName}, the parentName (which is the actual underlying field reference) leaks.
     * Verifies the FieldAttribute anonymization path anonymizes parentName too.
     */
    public void testFieldAttributeParentNameAnonymized() {
        String parent = "user_profile";
        String leaf = "email_address";
        EsField field = new EsField(leaf, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        // FieldAttribute with parentName set (the synthetic-attribute shape).
        FieldAttribute attr = new FieldAttribute(Source.EMPTY, parent, null, leaf, field, false);
        EsRelation relation = new EsRelation(
            Source.EMPTY,
            INDEX,
            IndexMode.STANDARD,
            Map.of(),
            Map.of(),
            Map.of(INDEX, IndexMode.STANDARD),
            List.<Attribute>of(attr)
        );
        LogicalPlan plan = new Limit(Source.EMPTY, new Literal(Source.EMPTY, 10, DataType.INTEGER), relation);

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, plan, null);

        // The display name 'email_address' is rewritten by withName already; verify the parentName
        // 'user_profile' doesn't survive — that's the synthetic-attribute leak we're guarding against.
        assertFalse("parentName leaked into optimized:\n" + out.optimized(), out.optimized().contains(parent));
        assertFalse("parentName leaked into schema:\n" + out.schema(), out.schema().contains(parent));
        assertFalse("leaf field name leaked into optimized:\n" + out.optimized(), out.optimized().contains(leaf));
    }

    /**
     * Parsed-plan path: build an UnresolvedRelation + UnresolvedAttribute fragment (the shape the
     * parser produces before analysis runs) with sensitive identifiers. Same property as the
     * optimized-plan adversarial test — nothing leaks into the artifacts.
     */
    public void testParsedPlanWithUnresolvedNodesAnonymized() {
        String sensitiveIndex = "prod-customer-pii-tenant-7";
        String sensitiveColumn = "user_ssn";
        IndexPattern pattern = new IndexPattern(Source.EMPTY, sensitiveIndex);
        UnresolvedRelation unresolved = new UnresolvedRelation(Source.EMPTY, pattern, false, List.of(), IndexMode.STANDARD, null);
        UnresolvedAttribute col = new UnresolvedAttribute(Source.EMPTY, sensitiveColumn);
        Literal lit = new Literal(Source.EMPTY, new BytesRef("555-12-3456"), DataType.KEYWORD);
        LogicalPlan parsed = new Filter(Source.EMPTY, unresolved, new Equals(Source.EMPTY, col, lit));

        var out = PlanAnonymizer.forSubmission(randomUUID()).anonymize(parsed, null, null, null);

        for (String secret : List.of(sensitiveIndex, sensitiveColumn, "555-12-3456")) {
            assertFalse("'" + secret + "' leaked into parsed plan:\n" + out.parsed(), out.parsed().contains(secret));
        }
        // Analyzed / optimized / physical not provided — should come back empty.
        assertEquals("", out.analyzed());
        assertEquals("", out.optimized());
        assertEquals("", out.physical());
        // Schema also empty when no analyzed/optimized plan is supplied.
        assertEquals("", out.schema());
    }

    /**
     * Thread safety: many threads each create their own anonymizer with the same cluster UUID and
     * anonymize the same plan concurrently. All threads must produce byte-identical output (column
     * tokens are HMAC-derived and deterministic across instances on the same cluster). Catches any
     * accidental static state leaking between anonymizer instances.
     */
    public void testConcurrentAnonymizationDifferentInstancesProduceSameOutput() throws Exception {
        LogicalPlan logical = sampleLogicalPlan();
        PhysicalPlan physical = new FragmentExec(logical);
        String clusterUuid = randomUUID();
        int threads = 16;
        int iters = 50;

        var expected = PlanAnonymizer.forSubmission(clusterUuid).anonymize(null, null, logical, physical);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<String> mismatches = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger ran = new AtomicInteger();
        try {
            for (int t = 0; t < threads; t++) {
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < iters; i++) {
                            var got = PlanAnonymizer.forSubmission(clusterUuid).anonymize(null, null, logical, physical);
                            if (expected.optimized().equals(got.optimized()) == false) {
                                mismatches.add("optimized mismatch");
                            }
                            if (expected.physical().equals(got.physical()) == false) {
                                mismatches.add("physical mismatch");
                            }
                            if (expected.schema().equals(got.schema()) == false) {
                                mismatches.add("schema mismatch");
                            }
                            ran.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue("threads did not finish in time", pool.awaitTermination(60, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }

        assertEquals(threads * iters, ran.get());
        assertTrue("concurrent anonymization produced inconsistent output: " + mismatches, mismatches.isEmpty());
    }

    /**
     * Thread safety: a single PlanAnonymizer instance is intended for single-submission single-thread
     * use, but its internal HashMaps must at least not corrupt under repeated sequential calls from
     * the same thread (anonymize called once per stage if we ever changed to per-stage anonymize).
     * This test exercises that contract.
     */
    public void testAnonymizerInstanceReentrantSequentialCalls() {
        LogicalPlan logical = sampleLogicalPlan();
        PhysicalPlan physical = new FragmentExec(logical);
        var anon = PlanAnonymizer.forSubmission(randomUUID());

        var first = anon.anonymize(null, null, logical, physical);
        var second = anon.anonymize(null, null, logical, physical);

        // Same instance, same input → same output. Column/index tokens are stable; literal ids are
        // monotonic but interned by (type, value) so identical input plans produce identical ids.
        assertEquals(first.optimized(), second.optimized());
        assertEquals(first.physical(), second.physical());
        assertEquals(first.schema(), second.schema());
    }

    /**
     * Null-safety: any of the four plan args may be null when the pipeline failed before that stage
     * completed. Empty strings come back for missing stages; no NPE.
     */
    public void testNullStagesProduceEmptySections() {
        LogicalPlan logical = sampleLogicalPlan();

        var allNull = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, null, null);
        assertEquals("", allNull.parsed());
        assertEquals("", allNull.analyzed());
        assertEquals("", allNull.optimized());
        assertEquals("", allNull.physical());
        assertEquals("", allNull.schema());

        var onlyOptimized = PlanAnonymizer.forSubmission(randomUUID()).anonymize(null, null, logical, null);
        assertEquals("", onlyOptimized.parsed());
        assertEquals("", onlyOptimized.analyzed());
        assertFalse(onlyOptimized.optimized().isEmpty());
        assertEquals("", onlyOptimized.physical());
        assertFalse(onlyOptimized.schema().isEmpty());
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
