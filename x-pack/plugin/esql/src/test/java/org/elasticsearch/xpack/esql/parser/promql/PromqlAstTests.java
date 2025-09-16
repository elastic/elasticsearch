/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.QlClientException;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.PromqlParser;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Test for checking the overall grammar by throwing a number of valid queries at the parser to see whether any exception is raised.
 * In time, the queries themselves get to be checked against the actual execution model and eventually against the expected results.
 */
public class PromqlAstTests extends ESTestCase {

    public void testValidQueries() throws Exception {
        List<Tuple<String, Integer>> lines = PromqlGrammarTests.readQueries("/promql/grammar/queries-valid.promql");
        for (Tuple<String, Integer> line : lines) {
            String q = line.v1();
            try {
                PromqlParser parser = new PromqlParser();
                LogicalPlan plan = parser.createStatement(q, null, null);
            } catch (ParsingException pe) {
                fail(
                    format(null,
                        "Error parsing line {}:{} '{}' [{}]",
                        line.v2(),
                        pe.getColumnNumber(),
                        pe.getErrorMessage(),
                        q
                    )
                );
            } catch (Exception e) {
                fail(format(null, "Unexpected exception for line {}: [{}] \n {}", line.v2(), line.v1(), e));
            }
        }
    }

    public void testQuery() throws Exception {
        String query = "rate(metric[5m])";
        new PromqlParser().createStatement(query);
    }

    @AwaitsFix(bugUrl = "placeholder for individual queries")
    public void testSingleQuery() throws Exception {
        String query = "{x=\".*\"}";
        new PromqlParser().createStatement(query);
    }

    //@AwaitsFix(bugUrl = "requires parsing validation, not the focus for now")
    public void testUnsupportedQueries() throws Exception {
        List<Tuple<String, Integer>> lines = PromqlGrammarTests.readQueries("/promql/grammar/queries-invalid.promql");
        for (Tuple<String, Integer> line : lines) {
            String q = line.v1();
            try {
                System.out.println("Testing invalid query: " + q);
                PromqlParser parser = new PromqlParser();
                Exception pe = expectThrowsAnyOf(
                    asList(QlClientException.class, UnsupportedOperationException.class),
                    () -> parser.createStatement(q)
                );
                parser.createStatement(q);
                //System.out.printf(pe.getMessage());
            } catch (QlClientException | UnsupportedOperationException ex) {
                // Expected
            }
//            } catch (AssertionError ae) {
//                fail(format(null, "Unexpected exception for line {}: [{}] \n {}", line.v2(), line.v1(), ae.getCause()));
//            }
        }
    }
}
