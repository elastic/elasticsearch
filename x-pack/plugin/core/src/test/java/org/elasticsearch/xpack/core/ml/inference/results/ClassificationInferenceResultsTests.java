/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfigTests;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class ClassificationInferenceResultsTests extends AbstractWireSerializingTestCase<ClassificationInferenceResults> {

    public static ClassificationInferenceResults createRandomResults() {
        return new ClassificationInferenceResults(randomDouble(),
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null :
                Stream.generate(ClassificationInferenceResultsTests::createRandomClassEntry)
                    .limit(randomIntBetween(0, 10))
                    .collect(Collectors.toList()),
            ClassificationConfigTests.randomClassificationConfig());
    }

    private static ClassificationInferenceResults.TopClassEntry createRandomClassEntry() {
        return new ClassificationInferenceResults.TopClassEntry(randomAlphaOfLength(10), randomDouble());
    }

    public void testWriteResultsWithClassificationLabel() {
        ClassificationInferenceResults result =
            new ClassificationInferenceResults(1.0, "foo", Collections.emptyList(), ClassificationConfig.EMPTY_PARAMS);
        IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
        result.writeResult(document, "result_field");

        assertThat(document.getFieldValue("result_field.predicted_value", String.class), equalTo("foo"));
    }

    public void testWriteResultsWithoutClassificationLabel() {
        ClassificationInferenceResults result = new ClassificationInferenceResults(1.0,
            null,
            Collections.emptyList(),
            ClassificationConfig.EMPTY_PARAMS);
        IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
        result.writeResult(document, "result_field");

        assertThat(document.getFieldValue("result_field.predicted_value", String.class), equalTo("1.0"));
    }

    @SuppressWarnings("unchecked")
    public void testWriteResultsWithTopClasses() {
        List<ClassificationInferenceResults.TopClassEntry> entries = Arrays.asList(
            new ClassificationInferenceResults.TopClassEntry("foo", 0.7),
            new ClassificationInferenceResults.TopClassEntry("bar", 0.2),
            new ClassificationInferenceResults.TopClassEntry("baz", 0.1));
        ClassificationInferenceResults result = new ClassificationInferenceResults(1.0,
            "foo",
            entries,
            new ClassificationConfig(3, "my_results", "bar"));
        IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
        result.writeResult(document, "result_field");

        List<?> list = document.getFieldValue("result_field.bar", List.class);
        assertThat(list.size(), equalTo(3));

        for(int i = 0; i < 3; i++) {
            Map<String, Object> map = (Map<String, Object>)list.get(i);
            assertThat(map, equalTo(entries.get(i).asValueMap()));
        }

        assertThat(document.getFieldValue("result_field.my_results", String.class), equalTo("foo"));
    }

    @Override
    protected ClassificationInferenceResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected Writeable.Reader<ClassificationInferenceResults> instanceReader() {
        return ClassificationInferenceResults::new;
    }
}
