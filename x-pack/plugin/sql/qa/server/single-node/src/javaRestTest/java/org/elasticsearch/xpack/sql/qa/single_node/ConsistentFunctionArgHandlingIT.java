/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.sql.qa.jdbc.JdbcIntegrationTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.lang.String.join;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.core.Tuple.tuple;
import static org.hamcrest.collection.IsEmptyCollection.empty;

/**
 * <p>This test was introduced because of the inconsistencies regarding NULL argument handling. NULL literal vs NULL field
 * value as function arguments in some case result in different function return values.</p>
 *
 * <p>Functions should return with the same value no matter if the argument(s) came from a field or from a literal.</p>
 *
 * <p>The test class based on the example function calls (and argument specifications) generates all the
 * permutations of the function arguments (4 options per argument: value/NULL as literal/field) and tests that the
 * function calls with the same argument values provide the same result regardless of the source (literal, field)
 * of the arguments.</p>
 *
 * <p>To ignore any of the tests, add an .ignore() method call after the Fn ctors in the FUNCTION_CALLS_TO_TEST list below, like:
 * <code> new Fn("ASCII", "foobar").ignore()</code></p>
 */
public class ConsistentFunctionArgHandlingIT extends JdbcIntegrationTestCase {

    private static final List<Fn> FUNCTION_CALLS_TO_TEST = asList(
        new Fn("ASCII", "foobar"),
        new Fn("BIT_LENGTH", "foobar"),
        new Fn("CHAR", 66),
        new Fn("CHAR_LENGTH", "foobar").aliases("CHARACTER_LENGTH"),
        new Fn("CONCAT", "foo", "bar"),
        new Fn("INSERT", "foobar", 2, 3, "replacement"),
        new Fn("LCASE", "STRING"),
        new Fn("LEFT", "foobar", 3),
        new Fn("LENGTH", "foobar"),
        new Fn("LOCATE", "ob", "foobar", 1),
        new Fn("LTRIM", "   foobar"),
        new Fn("OCTET_LENGTH", "foobar"),
        new Fn("POSITION", "foobar", "ob"),
        new Fn("REPEAT", "foobar", 10),
        new Fn("REPLACE", "foo", "o", "bar"),
        new Fn("RIGHT", "foobar", 3),
        new Fn("RTRIM", "foobar   "),
        new Fn("SPACE", 5),
        new Fn("STARTS_WITH", "foobar", "foo"),
        new Fn("SUBSTRING", "foobar", 1, 2),
        new Fn("TRIM", "  foobar   "),
        new Fn("UCASE", "foobar")
    );

    private static final List<String> NON_TESTED_FUNCTIONS;
    static {
        try {
            Class<?> c = ConsistentFunctionArgHandlingIT.class;
            NON_TESTED_FUNCTIONS = Files.readAllLines(
                PathUtils.get(c.getResource(c.getSimpleName() + "-non-tested-functions.txt").toURI())
            );
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private enum Source {
        FIELD,
        LITERAL
    }

    private static class Fn {
        private final String name;
        private final List<Argument> arguments;
        private List<String> aliases = new ArrayList<>();
        private boolean ignored = false;

        private Fn(String name, Object... arguments) {
            this.name = name;
            this.arguments = new ArrayList<>();
            for (Object a : arguments) {
                this.arguments.add(new Argument(a));
            }
        }

        public Fn aliases(String... aliases) {
            this.aliases = asList(aliases);
            return this;
        }

        public Fn ignore() {
            this.ignored = true;
            return this;
        }

        @Override
        public String toString() {
            return name + "(" + arguments.stream().map(a -> String.valueOf(a.exampleValue)).collect(joining(", ")) + ")";
        }
    }

    private static class Argument {
        private final Object exampleValue;
        private final Source[] acceptedSources;

        private Argument(Object exampleValue, Source... acceptedSources) {
            this.exampleValue = exampleValue;
            this.acceptedSources = acceptedSources.length == 0 ? Source.values() : acceptedSources;
        }
    }

    @ParametersFactory
    public static Iterable<Object[]> testFactory() {
        List<Object[]> tests = new ArrayList<>();
        tests.add(new Object[] { null });
        FUNCTION_CALLS_TO_TEST.forEach(f -> tests.add(new Object[] { f }));
        return tests;
    }

    private final Fn fn;

    public ConsistentFunctionArgHandlingIT(Fn fn) {
        this.fn = fn;
    }

    public void test() throws Exception {
        if (fn == null) {
            checkScalarFunctionCoverage();
            return;
        }

        assumeFalse("Ignored", fn.ignored);

        // create a record for the function, where all the example (non-null) argument values are stored in fields
        // so the field mapping is automatically set up
        final String functionName = fn.name;
        final String indexName = "test";
        final String argPrefix = "arg_" + functionName + "_";
        final String nullArgPrefix = "arg_null_" + functionName + "_";
        final String testDocId = functionName + "_" + UUIDs.base64UUID();

        indexTestDocForFunction(functionName, indexName, argPrefix, nullArgPrefix, testDocId);

        List<List<Object>> possibleValuesPerArguments = fn.arguments.stream().map(a -> asList(a.exampleValue, null)).collect(toList());
        List<List<Source>> acceptedSourcesPerArguments = fn.arguments.stream().map(a -> asList(a.acceptedSources)).collect(toList());

        iterateAllPermutations(possibleValuesPerArguments, argValues -> {
            // we only want to check the function calls that have at least a single NULL argument
            if (argValues.stream().noneMatch(Objects::isNull)) {
                return;
            }

            List<Tuple<String, Object>> results = new ArrayList<>();

            iterateAllPermutations(acceptedSourcesPerArguments, argSources -> {
                List<String> functionCallArgs = new ArrayList<>();
                List<String> functionCallArgsForAssert = new ArrayList<>();
                for (int argIndex = 0; argIndex < argValues.size(); argIndex++) {
                    final Object argValue = argValues.get(argIndex);
                    final Source argSource = argSources.get(argIndex);
                    final String valueAsLiteral = asLiteralInQuery(argValue);
                    switch (argSource) {
                        case LITERAL -> functionCallArgs.add(valueAsLiteral);
                        case FIELD -> {
                            final String argFieldName = (argValue == null ? nullArgPrefix : argPrefix) + (argIndex + 1);
                            functionCallArgs.add(argFieldName);
                        }
                    }
                    functionCallArgsForAssert.add(valueAsLiteral + "{" + argSource.name().charAt(0) + "}");
                }

                final String functionCall = functionName + "(" + join(", ", functionCallArgs) + ")";
                final String query = "SELECT " + functionCall + " FROM " + indexName + " WHERE docId = '" + testDocId + "'";
                ResultSet retVal = esJdbc().createStatement().executeQuery(query);

                assertTrue(retVal.next());
                results.add(tuple(functionName + "(" + join(", ", functionCallArgsForAssert) + ")", retVal.getObject(1)));
                // only a single row should be returned
                assertFalse(retVal.next());

                if (results.stream().map(Tuple::v2).distinct().count() > 1) {
                    int maxResultWidth = results.stream().map(Tuple::v2).mapToInt(o -> asLiteralInQuery(o).length()).max().orElse(20);
                    String resultsAsString = results.stream()
                        .map(r -> String.format(Locale.ROOT, "%2$-" + maxResultWidth + "s // %1$s", r.v1(), asLiteralInQuery(r.v2())))
                        .collect(joining("\n"));
                    fail("The result of the last call differs from the other calls:\n" + resultsAsString);
                }
            });
        });
    }

    private void indexTestDocForFunction(String functionName, String indexName, String argPrefix, String nullArgPrefix, String testDocId)
        throws IOException {
        Map<String, Object> testDoc = new LinkedHashMap<>();
        testDoc.put("docId", testDocId);
        int idx = 0;
        for (Argument arg : fn.arguments) {
            idx += 1;
            testDoc.put(argPrefix + idx, arg.exampleValue);
            // first set the same value, so the mapping is populated for the null columns
            testDoc.put(nullArgPrefix + idx, arg.exampleValue);
        }
        index(indexName, functionName, body -> body.mapContents(testDoc));

        // zero out the fields to be used as nulls
        for (idx = 1; idx <= fn.arguments.size(); idx++) {
            testDoc.put(nullArgPrefix + idx, null);
        }
        index(indexName, functionName, body -> body.mapContents(testDoc));
    }

    private void checkScalarFunctionCoverage() throws Exception {
        ResultSet resultSet = esJdbc().createStatement().executeQuery("SHOW FUNCTIONS");
        Set<String> functions = new LinkedHashSet<>();
        while (resultSet.next()) {
            String name = resultSet.getString(1);
            String fnType = resultSet.getString(2);
            if ("SCALAR".equals(fnType)) {
                functions.add(name);
            }
        }
        for (Fn fn : FUNCTION_CALLS_TO_TEST) {
            functions.remove(fn.name);
            functions.removeAll(fn.aliases);
        }
        functions.removeAll(NON_TESTED_FUNCTIONS);

        assertThat("Some functions are not covered by this test", functions, empty());
    }

    private static String asLiteralInQuery(Object argValue) {
        String argInQuery;
        if (argValue == null) {
            argInQuery = "NULL";
        } else {
            argInQuery = String.valueOf(argValue);
            if (argValue instanceof String) {
                argInQuery = "'" + argInQuery + "'";
            }
        }
        return argInQuery;
    }

    private static <T> void iterateAllPermutations(List<List<T>> possibleValuesPerItem, CheckedConsumer<List<T>, Exception> consumer)
        throws Exception {

        if (possibleValuesPerItem.isEmpty()) {
            consumer.accept(new ArrayList<>());
            return;
        }
        iterateAllPermutations(possibleValuesPerItem.subList(1, possibleValuesPerItem.size()), onePermutationOfTail -> {
            for (T option : possibleValuesPerItem.get(0)) {
                ArrayList<T> onePermutation = new ArrayList<>();
                onePermutation.add(option);
                onePermutation.addAll(onePermutationOfTail);
                consumer.accept(onePermutation);
            }
        });
    }

}
