/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexProperties;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.plan.logical.UriParts;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType.NONE;
import static org.hamcrest.Matchers.containsString;

/** Qualifies the request-pragmas verifier route without enabling URI_PARTS for ordinary stage callers. */
public class SequenceUriPartsAnalyzerTests extends ESTestCase {
    private static final String QUERY = """
        SEQUENCE BY id WITHIN 30s PRIMARY 1
        (FROM a | URI_PARTS parsed = url), (FROM b)
        | KEEP id, parsed.domain | LIMIT 10
        """;

    /** The candidate changes stage admission only when native SEQUENCE exists. */
    @BeforeClass
    public static void requireSequenceCommand() {
        assumeTrue("requires native SEQUENCE", EsqlCapabilities.Cap.SEQUENCE_COMMAND.isEnabled());
    }

    /** Absence and explicit false preserve the existing refusal through the real verifier. */
    public void testDefaultAndExplicitOffRefuseUriParts() {
        assertFalse(QueryPragmas.EMPTY.sequenceUriParts());
        assertTrue(QueryPragmas.VALID_PRAGMA_NAMES.contains("sequence_uri_parts"));
        configured(QueryPragmas.EMPTY).statementError(QUERY, containsString("[UriParts] is not supported inside a SEQUENCE stage"));
        configured(pragmas(false)).statementError(QUERY, containsString("[UriParts] is not supported inside a SEQUENCE stage"));
    }

    /** An explicit request reaches the pragma-aware callback and resolves ordinary URI output fields. */
    public void testExplicitOnAndNormalColumnPaths() {
        var plan = configured(pragmas(true)).statement(QUERY);
        assertEquals(1, plan.collect(UriParts.class).size());
        assertEquals(2, plan.collect(SequenceStage.class).size());
        assertEquals(List.of("id", "parsed.domain"), plan.output().stream().map(a -> a.name()).toList());
        configured(pragmas(true)).statement(
            QUERY.replace("URI_PARTS parsed = url", "EVAL alias = url | URI_PARTS parsed = alias | DROP alias, url")
        );
    }

    /** Identity scoping prevents a stage checker from validating other stage instances. */
    public void testCallbackIdentityAndDefaultOffEntrypoints() {
        var stages = configured(pragmas(true)).statement(QUERY).collect(SequenceStage.class);
        var first = stages.stream().filter(stage -> stage.child().collect(UriParts.class).isEmpty() == false).findFirst().orElseThrow();
        var other = stages.stream().filter(stage -> stage != first).findFirst().orElseThrow();
        Failures failures = new Failures();
        var on = first.postAnalysisPlanVerification(null, pragmas(true), EsqlFlags.DEFAULTS);
        on.accept(other, failures);
        assertFalse(failures.hasFailures());
        on.accept(first, failures);
        assertFalse(failures.hasFailures());
        first.postAnalysisPlanVerification().accept(first, failures);
        assertTrue(failures.hasFailures());
        Failures direct = new Failures();
        first.postAnalysisVerification(direct);
        assertTrue(direct.hasFailures());
    }

    /** URI admission must retain unrelated command refusal and the independent URI input-type check. */
    public void testOnDoesNotWeakenOtherChecks() {
        assertThat(
            expectThrows(
                ParsingException.class,
                () -> configured(pragmas(true)).statement(QUERY.replace("URI_PARTS parsed = url", "URI_PARTS parsed = url | LIMIT 1"))
            ).getMessage(),
            containsString("[LIMIT] is not supported inside a SEQUENCE stage")
        );
        configured(pragmas(true)).statementError(
            QUERY.replace("parsed = url", "parsed = number"),
            containsString("Input for URI_PARTS must be of type [string] but is [integer]")
        );
        configured(pragmas(true)).statementError(
            QUERY.replace("URI_PARTS parsed = url", "URI_PARTS parsed = url | EVAL @timestamp = TO_DATETIME(0)"),
            containsString("SEQUENCE stages cannot overwrite the source [@timestamp]")
        );
        configured(pragmas(true)).statementError(
            QUERY.replace("BY id", "BY location"),
            containsString("SEQUENCE BY column [location] has unsupported type [GEO_POINT]")
        );
    }

    private static QueryPragmas pragmas(boolean enabled) {
        return new QueryPragmas(Settings.builder().put("sequence_uri_parts", enabled).build());
    }

    private static TestAnalyzer configured(QueryPragmas pragmas) {
        Map<String, EsField> mapping = Map.of(
            "@timestamp",
            DateEsField.dateEsField("@timestamp", Map.of(), true, NONE),
            "id",
            new EsField("id", DataType.KEYWORD, Map.of(), true, NONE),
            "url",
            new EsField("url", DataType.KEYWORD, Map.of(), true, NONE),
            "number",
            new EsField("number", DataType.INTEGER, Map.of(), true, NONE),
            "location",
            new EsField("location", DataType.GEO_POINT, Map.of(), true, NONE)
        );
        return EsqlTestUtils.analyzer()
            .configuration(EsqlTestUtils.configuration(pragmas))
            .minimumTransportVersion(TransportVersion.current())
            .addIndex(new EsIndex("a", mapping, Map.of("a", new IndexProperties(IndexMode.STANDARD, 0)), Map.of(), Map.of()))
            .addIndex(new EsIndex("b", mapping, Map.of("b", new IndexProperties(IndexMode.STANDARD, 0)), Map.of(), Map.of()));
    }
}
