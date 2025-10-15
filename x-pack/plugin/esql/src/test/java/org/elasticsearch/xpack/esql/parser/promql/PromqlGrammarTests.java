/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.PromqlBaseLexer;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser;
import org.junit.Test;

import java.io.BufferedReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

public class PromqlGrammarTests extends ESTestCase {

    private void parse(String query) {
        CharStream input = CharStreams.fromString(query);
        PromqlBaseLexer lexer = new PromqlBaseLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        PromqlBaseParser parser = new PromqlBaseParser(tokens);

        parser.removeErrorListeners();
        parser.addErrorListener(new BaseErrorListener() {
            @Override
            public void syntaxError(
                Recognizer<?, ?> recognizer,
                Object offendingSymbol,
                int line,
                int charPositionInLine,
                String msg,
                RecognitionException e
            ) {
                throw new ParsingException(new Source(line, charPositionInLine, ""), "Syntax error: {}", msg);
            }
        });

        parser.expression();
    }

    private void validate(String query, int lineNumber) {
        try {
            parse(query);
        } catch (ParsingException pe) {
            fail(format(null, "Error parsing line {}:{} '{}' [{}]", lineNumber, pe.getColumnNumber(), pe.getErrorMessage(), query));
        } catch (Exception e) {
            fail(format(null, "Unexpected exception for line {}: [{}] \n {}", lineNumber, query, e));
        }
    }

    private void invalidate(String query, int lineNumber) {
        var exception = expectThrows(
            ParsingException.class,
            "No exception parsing line " + lineNumber + ":[" + query + "]",
            () -> parse(query)
        );
        assertThat(exception.getMessage(), containsString("Syntax error:"));
    }

    @Test
    public void testValidQueries() throws Exception {
        List<Tuple<String, Integer>> lines = readQueries("/promql/grammar/queries-valid.promql");
        for (Tuple<String, Integer> line : lines) {
            validate(line.v1(), line.v2());
        }
    }

    @Test
    // @AwaitsFix(bugUrl = "requires the parser to be implemented to perform validation")
    public void testInvalidQueries() throws Exception {
        List<Tuple<String, Integer>> lines = readQueries("/promql/grammar/queries-invalid.promql");
        for (Tuple<String, Integer> line : lines) {
            String q = line.v1();
            if (q.startsWith("|")) {
                // message line - skip for now
                continue;
            }
            invalidate(line.v1(), line.v2());
        }
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
