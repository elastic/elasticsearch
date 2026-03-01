/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.tree.Location;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.test.ESTestCase.assertThat;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractTestCaseHelper<S extends AbstractTestCaseHelper<S>> {
    private static final Logger log = LogManager.getLogger(AbstractTestCaseHelper.class);

    private final int arity;
    private final List<List<TestCaseSupplier.TypedDataSupplier>> values;
    private final Function<List<TestCaseSupplier.TypedDataSupplier>, String> name;
    private final String evaluatorToString;
    private final DataType outputType;
    /**
     * Map from inputs to expected value.
     */
    private final Function<List<Object>, Object> expected;
    /**
     * Map from inputs to expected warnings.
     */
    private final Function<List<Object>, List<String>> expectedWarnings;
    private final Supplier<Configuration> configuration;

    protected AbstractTestCaseHelper(int arity) {
        this.arity = arity;
        this.values = List.of();
        this.name = null;
        this.evaluatorToString = null;
        this.outputType = null;
        this.expected = null;
        this.expectedWarnings = v -> List.of();
        this.configuration = null;
    }

    protected AbstractTestCaseHelper(
        int arity,
        List<List<TestCaseSupplier.TypedDataSupplier>> values,
        Function<List<TestCaseSupplier.TypedDataSupplier>, String> name,
        String evaluatorToString,
        DataType expectedOutputType,
        Function<List<Object>, Object> expected,
        Function<List<Object>, List<String>> expectedWarnings,
        Supplier<Configuration> configuration
    ) {
        this.arity = arity;
        this.values = values;
        this.name = name;
        this.evaluatorToString = evaluatorToString;
        this.outputType = expectedOutputType;
        this.expected = expected;
        this.expectedWarnings = expectedWarnings;
        this.configuration = configuration;
    }

    /**
     * Create a new instance with the given parent fields, preserving subclass-specific state.
     */
    protected abstract S create(
        List<List<TestCaseSupplier.TypedDataSupplier>> values,
        Function<List<TestCaseSupplier.TypedDataSupplier>, String> name,
        String expectedEvaluatorToString,
        DataType expectedOutputType,
        Function<List<Object>, Object> expected,
        Function<List<Object>, List<String>> expectedWarnings,
        Supplier<Configuration> configuration
    );

    protected final List<List<TestCaseSupplier.TypedDataSupplier>> values() {
        return values;
    }

    protected final S addTestCases(List<List<TestCaseSupplier.TypedDataSupplier>> values) {
        List<List<TestCaseSupplier.TypedDataSupplier>> newValues = new ArrayList<>(this.values);
        newValues.addAll(values);
        return create(List.copyOf(newValues), name, evaluatorToString, outputType, expected, expectedWarnings, configuration);
    }

    /**
     * Provide a naming function for the cases this will build. If this is not provided
     * then the case will be named
     * <pre>{@code <param1name, param2name, param3name>}</pre>.
     */
    public final S name(Function<List<TestCaseSupplier.TypedDataSupplier>, String> name) {
        return create(values, name, this.evaluatorToString, this.outputType, this.expected, this.expectedWarnings, this.configuration);
    }

    /**
     * Sets the expected {@link Object#toString} of the {@link EvalOperator.ExpressionEvaluator}.
     * Use {@code %0} for the "reader" for the first parameter. Use {@code %1} for the second. Etc.
     */
    public final S evaluatorToString(String evaluatorToString) {
        return create(values, name, evaluatorToString, outputType, expected, expectedWarnings, configuration);
    }

    protected final String evaluatorToString() {
        return evaluatorToString;
    }

    public final S expectedOutputType(DataType expectedOutputType) {
        return create(values, name, evaluatorToString, expectedOutputType, expected, expectedWarnings, configuration);
    }

    protected final S expectedFromArgs(Function<List<Object>, Object> expected) {
        return create(values, name, evaluatorToString, outputType, expected, expectedWarnings, configuration);
    }

    protected final S expectWarningsFromArgs(Function<List<Object>, List<String>> expectedWarnings) {
        Function<List<Object>, List<String>> wrapped = o -> {
            List<String> e = expectedWarnings.apply(o);
            if (e.isEmpty()) {
                return e;
            }
            List<String> warnings = new ArrayList<>();
            warnings.add("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.");
            warnings.addAll(e);
            return warnings;
        };
        return create(values, name, evaluatorToString, outputType, expected, wrapped, configuration);
    }

    /**
     * Provide a {@link Configuration} to the function. If this isn't called
     * then a {@linkplain Configuration} is not available to the function.
     */
    public final S configuration(Supplier<Configuration> configuration) {
        return create(values, name, evaluatorToString, outputType, expected, expectedWarnings, configuration);
    }

    /**
     * Build the {@link TestCaseSupplier suppliers} and write them into the provided list.
     */
    public final void build(List<TestCaseSupplier> suppliers) {
        if (values.isEmpty()) {
            throw new IllegalStateException("values must be provided");
        }
        if (evaluatorToString == null) {
            throw new IllegalStateException("evaluatorToString must be provided");
        }
        if (outputType == null) {
            throw new IllegalStateException("expectedOutputType must be provided");
        }
        if (expected == null) {
            throw new IllegalStateException("expectedValue must be provided");
        }
        for (List<TestCaseSupplier.TypedDataSupplier> valueSuppliers : values) {
            List<DataType> types = valueSuppliers.stream().map(TestCaseSupplier.TypedDataSupplier::type).toList();
            Supplier<TestCaseSupplier.TestCase> supplier = () -> buildTestCase(
                valueSuppliers.stream().map(TestCaseSupplier.TypedDataSupplier::get).toList()
            );
            suppliers.add(new TestCaseSupplier(resolveName(valueSuppliers), types, supplier));
        }
    }

    /**
     * Pick a name for the test case represented by the provided parameters.
     */
    private String resolveName(List<TestCaseSupplier.TypedDataSupplier> valueSuppliers) {
        if (name != null) {
            // name pattern was configured, don't use the default
            return name.apply(valueSuppliers);
        }
        StringBuilder b = new StringBuilder();
        for (TestCaseSupplier.TypedDataSupplier s : valueSuppliers) {
            if (b.isEmpty() == false) {
                b.append(", ");
            }
            b.append(s.name());
        }
        return b.toString();
    }

    /**
     * Build a {@link TestCaseSupplier.TestCase} with the provided parameters.
     */
    private TestCaseSupplier.TestCase buildTestCase(List<TestCaseSupplier.TypedData> parameters) {
        log.info("Inputs are {}", parameters);
        List<Object> inputs = parameters.stream().map(TestCaseSupplier.TypedData::getValue).toList();
        assertThat(parameters.size(), equalTo(arity));
        var expectedValue = this.expected.apply(inputs);
        log.info("expectedValue is {}", expectedValue);
        var matcher = expectedValue instanceof Matcher<?> ? (Matcher<?>) expectedValue : equalTo(expectedValue);
        TestCaseSupplier.TestCase testCase = new TestCaseSupplier.TestCase(parameters, resolveEvaluatorToString(), outputType, matcher);
        for (String warning : expectedWarnings.apply(inputs)) {
            testCase = testCase.withWarning(warning);
        }
        if (configuration != null) {
            testCase = testCase.withConfiguration(new Source(new Location(1, 0), "source"), configuration.get());
        }
        return testCase;
    }

    private String resolveEvaluatorToString() {
        String resolved = evaluatorToString;
        for (int a = 0; a < arity; a++) {
            resolved = resolved.replace("%" + a, "Attribute[channel=" + a + "]");
        }
        return resolved;
    }
}
