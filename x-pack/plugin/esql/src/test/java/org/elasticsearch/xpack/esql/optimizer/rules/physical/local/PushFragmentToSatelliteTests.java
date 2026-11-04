/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.compute.operator.PlanTimeProfile;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceResolution;
import org.elasticsearch.xpack.esql.datasources.FormatReaderRegistry;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.SatelliteFragment;
import org.elasticsearch.xpack.esql.datasources.spi.SimpleSourceMetadata;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Versioned;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.referenceAttribute;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

/** Runs real queries through parser, analyzer and optimizers and checks the fragment {@link PushFragmentToSatellite} produces. */
public class PushFragmentToSatelliteTests extends ESTestCase {

    private static final String PATH = "flight://localhost:1/employees";

    private static final String EMP_NO_SALARY =
        "\"output\":[{\"name\":\"emp_no\",\"type\":\"integer\"},{\"name\":\"salary\",\"type\":\"integer\"}]";
    private static final String SCAN_EMP_NO_SALARY =
        "{\"op\":\"scan\",\"target\":\"employees\",\"columns\":[{\"name\":\"emp_no\",\"type\":\"integer\"},"
            + "{\"name\":\"salary\",\"type\":\"integer\"}]}";
    private static final String SALARY_GT_50000 =
        "{\"fn\":\"greater_than\",\"type\":\"boolean\",\"args\":[{\"col\":\"salary\"},{\"lit\":50000,\"type\":\"integer\"}]}";

    public void testFilterProjectLimit() throws IOException {
        assertEquals(
            "{\"specVersion\":0,\"format\":\"json\",\"settings\":{\"locale\":\"en-US\",\"timeZone\":\"UTC\"},"
                + EMP_NO_SALARY
                + ",\"plan\":{\"op\":\"limit\",\"count\":10,\"input\":{\"op\":\"filter\",\"condition\":"
                + SALARY_GT_50000
                + ",\"input\":"
                + SCAN_EMP_NO_SALARY
                + "}}}",
            fragment("WHERE salary > 50000 | KEEP emp_no, salary | LIMIT 10", CAPS)
        );
    }

    public void testEvalAndFilterAboveEval() throws IOException {
        String f = fragment("WHERE salary > 50000 | EVAL x = salary * 2 | WHERE x > 3 | KEEP emp_no, x | LIMIT 10", CAPS);
        assertThat(f, containsString("{\"op\":\"eval\",\"fields\":[{\"name\":\"x\",\"expr\":{\"fn\":\"mul\",\"type\":\"integer\""));
        assertThat(f, containsString("\"condition\":{\"fn\":\"greater_than\",\"type\":\"boolean\",\"args\":[{\"col\":\"x\"}"));
        assertThat(f, startsWith("{\"specVersion\":0,\"format\":\"json\",\"settings\""));
        assertThat(f, containsString("\"plan\":{\"op\":\"limit\",\"count\":10"));
    }

    public void testTopNIsTheRootAndKeepsEsDefaultNullOrdering() throws IOException {
        String f = fragment("WHERE salary > 50000 | SORT salary DESC, emp_no | LIMIT 5", CAPS);
        assertThat(
            f,
            containsString(
                "\"plan\":{\"op\":\"topn\",\"order\":[{\"expr\":{\"col\":\"salary\"},\"direction\":\"desc\",\"nulls\":\"first\"},"
                    + "{\"expr\":{\"col\":\"emp_no\"},\"direction\":\"asc\",\"nulls\":\"last\"}],\"limit\":5,\"input\":{\"op\":\"filter\""
            )
        );
    }

    public void testLikePrefixBecomesStartsWithAndInIsPushed() throws IOException {
        String f = fragment("WHERE first_name LIKE \"A*\" AND languages IN (1, 2) | KEEP emp_no | LIMIT 10", CAPS);
        assertThat(
            f,
            containsString(
                "{\"fn\":\"starts_with\",\"type\":\"boolean\",\"args\":[{\"col\":\"first_name\"},{\"lit\":\"A\",\"type\":\"keyword\"}]}"
            )
        );
        assertThat(
            f,
            containsString(
                "{\"fn\":\"in\",\"type\":\"boolean\",\"args\":[{\"col\":\"languages\"},{\"lit\":1,\"type\":\"integer\"},"
                    + "{\"lit\":2,\"type\":\"integer\"}]}"
            )
        );
    }

    public void testUntranslatableConjunctStaysInEsAndStopsTheWalk() throws IOException {
        String f = fragment("WHERE salary > 50000 AND MV_COUNT(first_name) > 0 | KEEP emp_no, salary | LIMIT 10", CAPS);
        assertThat(f, containsString("\"plan\":{\"op\":\"filter\",\"condition\":" + SALARY_GT_50000));
        assertThat(f, not(containsString("\"limit\"")));
        assertThat(f, not(containsString("mv_count")));
    }

    public void testLongAndDatetimeLiteralsAreDecimalStrings() throws IOException {
        String f = fragment("WHERE hire_date > \"1990-01-01T00:00:00Z\"::datetime AND still_hired | KEEP emp_no | LIMIT 3", CAPS);
        assertThat(f, containsString("{\"lit\":\"631152000000\",\"type\":\"datetime\"}"));
        assertThat(f, containsString("{\"col\":\"still_hired\"}]"));
    }

    public void testDoubleLiteral() throws IOException {
        assertThat(fragment("WHERE height > 1.5 | KEEP emp_no | LIMIT 3", CAPS), containsString("{\"lit\":1.5,\"type\":\"double\"}"));
    }

    public void testScanAloneUnderLimit() throws IOException {
        assertEquals(
            "{\"specVersion\":0,\"format\":\"json\",\"settings\":{\"locale\":\"en-US\",\"timeZone\":\"UTC\"},"
                + EMP_NO_SALARY
                + ",\"plan\":{\"op\":\"limit\",\"count\":10,\"input\":"
                + SCAN_EMP_NO_SALARY
                + "}}",
            fragment("KEEP emp_no, salary | LIMIT 10", CAPS)
        );
    }

    public void testPragmaTurnsPushdownOff() throws IOException {
        assumeTrue("EXTERNAL command", org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());
        QueryPragmas off = new QueryPragmas(org.elasticsearch.common.settings.Settings.builder().put("satellite_pushdown", false).build());
        assertEquals("no fragment", run("EXTERNAL \"" + PATH + "\" | WHERE salary > 50000 | LIMIT 10", CAPS, off));
    }

    private static Map<String, Object> capsWithOptions() {
        Map<String, Object> caps = new java.util.HashMap<>(CAPS);
        caps.put("options", List.of("units"));
        return caps;
    }

    private static String fragmentWithOption(String query, Map<String, Object> caps, QueryPragmas pragmas) throws IOException {
        assumeTrue("EXTERNAL command", org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());
        return run("EXTERNAL \"" + PATH + "\" | " + query, caps, pragmas, Map.of("units", "fahrenheit"));
    }

    public void testDeclaredOptionsTravelInTheFragment() throws IOException {
        String f = fragmentWithOption("WHERE salary > 50000 | KEEP emp_no, salary | LIMIT 10", capsWithOptions(), QueryPragmas.EMPTY);
        assertThat(f, containsString("\"options\":{\"units\":\"fahrenheit\"}"));
        assertThat("the chain is still pushed", f, containsString("\"op\":\"filter\""));
    }

    public void testASourceWithOptionsIsReadThroughAFragmentEvenWithThePushdownOff() throws IOException {
        QueryPragmas off = new QueryPragmas(org.elasticsearch.common.settings.Settings.builder().put("satellite_pushdown", false).build());
        String f = fragmentWithOption("WHERE salary > 50000 | LIMIT 10", capsWithOptions(), off);
        assertThat("options need the fragment", f, containsString("\"options\":{\"units\":\"fahrenheit\"}"));
        assertThat("but nothing else is pushed", f, containsString("\"plan\":{\"op\":\"scan\""));
        assertThat(f, not(containsString("\"filter\"")));
        assertThat(f, not(containsString("\"limit\"")));
    }

    public void testASourceWithOptionsIsReadThroughAScanOnlyFragmentWhenNothingIsPushable() throws IOException {
        String f = fragmentWithOption("WHERE MV_COUNT(first_name) > 0 | LIMIT 10", capsWithOptions(), QueryPragmas.EMPTY);
        assertThat(f, containsString("\"options\":{\"units\":\"fahrenheit\"}"));
        assertThat(f, containsString("\"plan\":{\"op\":\"scan\""));
    }

    public void testASourceReadByAnOperatorThatReadsNoColumnIsRefusedWithAClearMessageWhenItHasOptions() throws IOException {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> fragmentWithOption("STATS c = COUNT(*) | LIMIT 5", capsWithOptions(), QueryPragmas.EMPTY)
        );
        assertThat(e.getMessage(), containsString("reads no column of the source"));
    }

    public void testWithoutOptionsAnOperatorThatReadsNoColumnIsLeftToThePlainScan() throws IOException {
        assertEquals("no fragment", fragment("STATS c = COUNT(*) | LIMIT 5", CAPS));
    }

    public void testOptionsThatCannotBeSentFailTheQueryInsteadOfBeingDropped() throws IOException {
        Map<String, Object> caps = capsWithOptions();
        caps.put("strictColumns", List.of("emp_no", "first_name", "languages", "height", "hire_date", "still_hired")); // not salary
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> fragmentWithOption("WHERE salary > 50000 | KEEP emp_no, salary | LIMIT 10", caps, QueryPragmas.EMPTY)
        );
        assertThat(e.getMessage(), containsString("source options"));
        assertThat(e.getMessage(), containsString("type the protocol cannot carry"));
    }

    public void testAnOptionTheSatelliteDidNotDeclareIsNotForwarded() throws IOException {
        String f = fragmentWithOption("WHERE salary > 50000 | KEEP emp_no, salary | LIMIT 10", CAPS, QueryPragmas.EMPTY);
        assertThat(f, not(containsString("options")));
    }

    public void testNothingIsPushedWithoutTheCapabilityStamp() throws IOException {
        assertEquals("no fragment", fragment("WHERE salary > 50000 | LIMIT 10", null));
    }

    public void testUndeclaredFunctionIsNotPushed() throws IOException {
        Map<String, Object> caps = new java.util.HashMap<>(CAPS);
        caps.put("functions", List.of("and", "or", "not"));
        assertEquals("no fragment", fragment("WHERE salary > 50000 | LIMIT 10", caps));
    }

    public void testUndeclaredOperatorIsNotPushed() throws IOException {
        Map<String, Object> caps = new java.util.HashMap<>(CAPS);
        caps.put("plan", List.of("scan", "filter"));
        String f = fragment("WHERE salary > 50000 | LIMIT 10", caps);
        assertThat(f, containsString("\"plan\":{\"op\":\"filter\""));
        assertThat(f, not(containsString("\"limit\"")));
    }

    public void testListColumnIsNeverUsedInAnExpression() throws IOException {
        Map<String, Object> caps = new java.util.HashMap<>(CAPS);
        caps.put("listColumns", List.of("first_name"));
        assertEquals("no fragment", fragment("WHERE starts_with(first_name, \"A\") | LIMIT 3", caps));
    }

    public void testColumnOutsideTheStrictArrowTypesIsNeverScannedInAFragment() throws IOException {
        Map<String, Object> caps = new java.util.HashMap<>(CAPS);
        caps.put("strictColumns", List.of("emp_no", "first_name", "languages", "height", "hire_date", "still_hired")); // not salary
        assertEquals("no fragment", fragment("WHERE salary > 50000 | KEEP emp_no, salary | LIMIT 10", caps));
        // a query that does not read the column still pushes
        assertThat(fragment("WHERE emp_no > 5 | KEEP emp_no | LIMIT 10", caps), containsString("\"op\":\"scan\""));
    }

    public void testFragmentAboveTheDeclaredSizeIsNotPushed() throws IOException {
        Map<String, Object> caps = new java.util.HashMap<>(CAPS);
        caps.put("limits", Map.of("maxFragmentBytes", 2048)); // below the room kept for the partitions entry
        assertEquals("no fragment", fragment("WHERE salary > 50000 | LIMIT 10", caps));
        caps.put("limits", Map.of("maxFragmentBytes", 1024 * 1024));
        assertThat(fragment("WHERE salary > 50000 | LIMIT 10", caps), containsString("\"op\":\"limit\""));
    }

    public void testDeclarationWithoutSpecVersionZeroMeansNoPushdown() throws IOException {
        Map<String, Object> caps = new java.util.HashMap<>(CAPS);
        caps.put("specVersions", List.of(1));
        assertEquals("no fragment", fragment("WHERE salary > 50000 | LIMIT 10", caps));
    }

    private static final Map<String, Object> CAPS = Map.of(
        "specVersions",
        List.of(0),
        "fragmentFormats",
        List.of("json"),
        "plan",
        List.of("scan", "filter", "project", "eval", "topn", "limit"),
        "functions",
        List.of(
            "and",
            "or",
            "not",
            "equals",
            "not_equals",
            "less_than",
            "less_than_or_equal",
            "greater_than",
            "greater_than_or_equal",
            "in",
            "is_null",
            "is_not_null",
            "add",
            "sub",
            "mul",
            "neg",
            "abs",
            "starts_with",
            "ends_with",
            "contains",
            "coalesce",
            "like"
        ),
        "types",
        List.of("integer", "long", "double", "boolean", "keyword", "datetime", "date_nanos"),
        "listColumns",
        List.of(),
        "strictColumns",
        List.of("emp_no", "first_name", "languages", "salary", "height", "hire_date", "still_hired")
    );

    private static String fragment(String query, Map<String, Object> caps) throws IOException {
        assumeTrue("EXTERNAL command", org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXTERNAL_COMMAND.isEnabled());
        return run("EXTERNAL \"" + PATH + "\" | " + query, caps);
    }

    static String run(String query, Map<String, Object> caps) throws IOException {
        return run(query, caps, QueryPragmas.EMPTY);
    }

    static String run(String query, Map<String, Object> caps, QueryPragmas pragmas) throws IOException {
        return run(query, caps, pragmas, Map.of());
    }

    static String run(String query, Map<String, Object> caps, QueryPragmas pragmas, Map<String, Object> extraConfig) throws IOException {
        var statement = EsqlTestUtils.TEST_PARSER.createStatement(query);
        List<Attribute> schema = List.of(
            referenceAttribute("emp_no", DataType.INTEGER),
            referenceAttribute("first_name", DataType.KEYWORD),
            referenceAttribute("languages", DataType.INTEGER),
            referenceAttribute("salary", DataType.INTEGER),
            referenceAttribute("height", DataType.DOUBLE),
            referenceAttribute("hire_date", DataType.DATETIME),
            referenceAttribute("still_hired", DataType.BOOLEAN)
        );
        Map<String, Object> config = new java.util.HashMap<>(Map.of("endpoint", "flight://localhost:1", "target", "employees"));
        config.putAll(extraConfig);
        var sm = new SimpleSourceMetadata(
            schema,
            "flight",
            PATH,
            null,
            null,
            caps == null ? null : Map.of(SatelliteFragment.CAPABILITY_KEY, caps),
            config
        );
        ExternalSourceMetadata esm = new ExternalSourceMetadata() {
            @Override
            public String location() {
                return PATH;
            }

            @Override
            public List<Attribute> schema() {
                return sm.schema();
            }

            @Override
            public String sourceType() {
                return "flight";
            }

            @Override
            public Map<String, Object> sourceMetadata() {
                return sm.sourceMetadata();
            }

            @Override
            public Map<String, Object> config() {
                return sm.config();
            }
        };
        var resolution = new ExternalSourceResolution(
            Map.of(PATH, new ExternalSourceResolution.ResolvedSource(esm, FileList.UNRESOLVED, Map.of()))
        );
        var analyzer = analyzer().externalSourceResolution(resolution).buildAnalyzer();
        var conf = EsqlTestUtils.configuration(pragmas, query, statement);
        LogicalPlan analyzed = analyzer.analyze(statement.plan());
        var tv = TransportVersion.current();
        var optimizer = new LogicalPlanOptimizer(EsqlTestUtils.logicalOptimizerContext(conf, FoldContext.small(), tv));
        LogicalPlan logical = optimizer.optimize(analyzed);
        PhysicalPlan physical = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(conf, tv)).optimize(
            new Mapper().map(new Versioned<>(logical, tv))
        );
        PhysicalPlan local = PlannerUtils.localPlan(
            PlannerSettings.DEFAULTS,
            new EsqlFlags(true),
            conf,
            conf.newFoldContext(),
            physical,
            EsqlTestUtils.TEST_SEARCH_STATS,
            new FormatReaderRegistry(null),
            List.of(),
            new PlanTimeProfile()
        );
        List<ExternalSourceExec> leaves = local.collect(ExternalSourceExec.class);
        assertThat(leaves.size(), org.hamcrest.Matchers.equalTo(1));
        return leaves.get(0).pushedFilter() instanceof SatelliteFragment f ? json(f.request()) : "no fragment";
    }

    static String json(Map<String, Object> request) throws IOException {
        try (var builder = XContentFactory.jsonBuilder()) {
            return BytesReference.bytes(builder.map(request)).utf8ToString();
        }
    }
}
