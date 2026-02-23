/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.QlClientException;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.PromqlParser;
import org.elasticsearch.xpack.esql.plan.logical.Explain;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlCommand;

import java.io.BufferedReader;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

/**
 * Test for checking the overall grammar by throwing a number of valid queries at the parser to see whether any exception is raised.
 * In time, the queries themselves get to be checked against the actual execution model and eventually against the expected results.
 */
// @TestLogging(reason = "debug", value = "org.elasticsearch.xpack.esql.parser.promql:TRACE")
public class PromqlAstTests extends ESTestCase {

    private static final Logger log = LogManager.getLogger(PromqlAstTests.class);

    public void testValidQueries() throws Exception {
        testValidQueries("/promql/grammar/queries-valid.promql");
    }

    @AwaitsFix(bugUrl = "functionality not implemented yet")
    public void testValidQueriesNotYetWorkingDueToMissingFunctionality() throws Exception {
        testValidQueries("/promql/grammar/queries-valid-extra.promql");
    }

    private void testValidQueries(String url) throws Exception {
        List<Tuple<String, Integer>> lines = readQueries(url);
        for (Tuple<String, Integer> line : lines) {
            String q = line.v1();
            try {
                PromqlParser parser = new PromqlParser();
                Literal now = new Literal(Source.EMPTY, Instant.now(), DataType.DATETIME);
                var plan = parser.createStatement(q, now, now, 0, 0);
                log.trace("{}", plan);
                List.of(
                    "PROMQL index=test start=0 end=1 step=1m (%s)",
                    "PROMQL index=test start=0 end=1 step=1m foo=(%s)",
                    "PROMQL index=test start=0 end=1 step=1m %s",
                    "PROMQL time=0 %s"
                ).forEach(pattern -> {
                    var query = String.format(Locale.ROOT, pattern, q);
                    LogicalPlan esqlPlan = EsqlParser.INSTANCE.parseQuery(query);
                    assertThat(esqlPlan.collect(PromqlCommand.class), hasSize(1));

                    if (EsqlCapabilities.Cap.EXPLAIN.isEnabled()) {
                        LogicalPlan explainPlan = EsqlParser.INSTANCE.parseQuery("EXPLAIN (" + query + ")");
                        Explain explain = explainPlan.collect(Explain.class).getFirst();
                        assertThat(explain.query().collect(PromqlCommand.class), hasSize(1));

                        explainPlan = EsqlParser.INSTANCE.parseQuery("EXPLAIN (" + query + " | LIMIT 1 )");
                        explain = explainPlan.collect(Explain.class).getFirst();
                        assertThat(explain.query().collect(PromqlCommand.class), hasSize(1));
                    }
                });
            } catch (ParsingException pe) {
                fail(format(null, "Error parsing line {}:{} '{}' [{}]", line.v2(), pe.getColumnNumber(), pe.getErrorMessage(), q));
            } catch (Exception e) {
                fail(format(null, "Unexpected exception for line {}: [{}] \n {}", line.v2(), line.v1(), e));
            }
        }
    }

    public void testUnsupportedQueries() throws Exception {
        PromqlParser parser = new PromqlParser();
        List<Tuple<String, Integer>> lines = readQueries("/promql/grammar/queries-invalid.promql");
        for (Tuple<String, Integer> line : lines) {
            String q = line.v1();
            try {
                log.trace("Testing invalid query {}", q);
                parser.createStatement(q);
            } catch (QlClientException | UnsupportedOperationException ex) {
                // Expected
                log.trace("{}", ex.getMessage());
                continue;
            }
            fail("Expected exception for line " + line.v2() + ": [" + q + "] but none was thrown");
        }
    }

    @AwaitsFix(bugUrl = "placeholder for individual queries")
    public void testSingleQuery() throws Exception {
        // rate(http_requests_total[5m])[30m:1m+1^2%1]
        String query = """
            bar + on(foo) bla / on(baz, buz) group_right(test) blub
            """;
        var plan = new PromqlParser().createStatement(query);
        log.info("{}", plan);
    }

    static List<Tuple<String, Integer>> readQueries(String source) throws Exception {
        var urls = EsqlTestUtils.classpathResources(source);
        assertThat(urls, not(empty()));
        List<Tuple<String, Integer>> queries = new ArrayList<>();

        StringBuilder query = new StringBuilder();
        for (URL url : urls) {
            try (BufferedReader reader = EsqlTestUtils.reader(url)) {
                String line;
                int lineNumber = 1;

                while ((line = reader.readLine()) != null) {
                    // ignore comments
                    if (line.isEmpty() == false && line.startsWith("//") == false) {
                        query.append(line);

                        if (line.endsWith(";")) {
                            query.setLength(query.length() - 1);
                            queries.add(new Tuple<>(query.toString(), lineNumber));
                            query.setLength(0);
                        } else {
                            query.append("\n");
                        }
                    }
                    lineNumber++;
                }
            }
        }
        return queries;
    }
}
