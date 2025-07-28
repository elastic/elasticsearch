/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.TestIngestDocument;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.inference.InferenceResults.writeResult;
import static org.hamcrest.Matchers.equalTo;

public class NlpClassificationInferenceResultsTests extends InferenceResultsTestCase<NlpClassificationInferenceResults> {

    public static NlpClassificationInferenceResults createRandomResults() {
        return new NlpClassificationInferenceResults(
            randomAlphaOfLength(10),
            randomBoolean()
                ? null
                : Stream.generate(TopClassEntryTests::createRandomTopClassEntry)
                    .limit(randomIntBetween(0, 10))
                    .collect(Collectors.toList()),
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomDoubleBetween(0.0, 1.0, false),
            randomBoolean()
        );
    }

    @SuppressWarnings("unchecked")
    public void testWriteResultsWithTopClasses() {
        List<TopClassEntry> entries = Arrays.asList(
            new TopClassEntry("foo", 0.7, 0.7),
            new TopClassEntry("bar", 0.2, 0.2),
            new TopClassEntry("baz", 0.1, 0.1)
        );
        NlpClassificationInferenceResults result = new NlpClassificationInferenceResults(
            "foo",
            entries,
            "my_results",
            0.7,
            randomBoolean()
        );
        IngestDocument document = TestIngestDocument.emptyIngestDocument();
        writeResult(result, document, "result_field", "test");

        List<?> list = document.getFieldValue("result_field.top_classes", List.class);
        assertThat(list.size(), equalTo(3));

        for (int i = 0; i < 3; i++) {
            Map<String, Object> map = (Map<String, Object>) list.get(i);
            assertThat(map, equalTo(entries.get(i).asValueMap()));
        }

        assertThat(document.getFieldValue("result_field.my_results", String.class), equalTo("foo"));
    }

    @Override
    protected NlpClassificationInferenceResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected NlpClassificationInferenceResults mutateInstance(NlpClassificationInferenceResults instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<NlpClassificationInferenceResults> instanceReader() {
        return NlpClassificationInferenceResults::new;
    }

    @Override
    void assertFieldValues(
        NlpClassificationInferenceResults createdInstance,
        IngestDocument document,
        String parentField,
        String resultsField
    ) {
        String path = parentField + resultsField;
        assertThat(document.getFieldValue(path, String.class), equalTo(createdInstance.predictedValue()));
    }
}
