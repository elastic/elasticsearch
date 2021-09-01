/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;
import static org.hamcrest.Matchers.equalTo;

public abstract class PreProcessingTests<T extends PreProcessor> extends AbstractSerializingTestCase<T> {

    protected boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(new MlInferenceNamedXContentProvider().getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }

    void testProcess(PreProcessor preProcessor, Map<String, Object> fieldValues, Map<String, Matcher<? super Object>> assertions) {
        preProcessor.process(fieldValues);
        assertions.forEach((fieldName, matcher) ->
            assertThat(fieldValues.get(fieldName), matcher)
        );
    }

    public void testInputOutputFieldOrderConsistency() throws IOException {
        xContentTester(this::createParser, this::createXContextTestInstance, getToXContentParams(), this::doParseInstance)
            .numberOfTestRuns(NUMBER_OF_TEST_RUNS)
            .supportsUnknownFields(supportsUnknownFields())
            .shuffleFieldsExceptions(getShuffleFieldsExceptions())
            .randomFieldsExcludeFilter(getRandomFieldsExcludeFilter())
            .assertEqualsConsumer(this::assertFieldConsistency)
            .assertToXContentEquivalence(false)
            .test();
    }

    private void assertFieldConsistency(T lft, T rgt) {
        assertThat(lft.inputFields(), equalTo(rgt.inputFields()));
        assertThat(lft.outputFields(), equalTo(rgt.outputFields()));
    }

    public void testWithMissingField() {
        Map<String, Object> fields = randomFieldValues();
        PreProcessor preProcessor = this.createTestInstance();
        Map<String, Object> fieldsCopy = new HashMap<>(fields);
        preProcessor.process(fields);
        assertThat(fieldsCopy, equalTo(fields));
    }

    Map<String, Object> randomFieldValues() {
        int numFields = randomIntBetween(1, 5);
        Map<String, Object> fieldValues = new HashMap<>(numFields);
        for (int k = 0; k < numFields; k++) {
            fieldValues.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
        }
        return fieldValues;
    }

    Map<String, Object> randomFieldValues(String categoricalField, Object categoricalValue) {
        Map<String, Object> fieldValues = randomFieldValues();
        fieldValues.put(categoricalField, categoricalValue);
        return fieldValues;
    }

}
