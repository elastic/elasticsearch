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

import static org.elasticsearch.xpack.core.ml.inference.results.InferenceResults.writeResult;
import static org.hamcrest.Matchers.equalTo;

public class QuestionAnsweringInferenceResultsTests extends InferenceResultsTestCase<QuestionAnsweringInferenceResults> {

    public static QuestionAnsweringInferenceResults createRandomResults() {
        return new QuestionAnsweringInferenceResults(
            randomAlphaOfLength(10),
            randomInt(1000),
            randomInt(1000),
            randomBoolean()
                ? null
                : Stream.generate(TopAnswerEntryTests::createRandomTopAnswerEntry)
                    .limit(randomIntBetween(0, 10))
                    .collect(Collectors.toList()),
            randomAlphaOfLength(10),
            randomDoubleBetween(0.0, 1.0, false),
            randomBoolean()
        );
    }

    @SuppressWarnings("unchecked")
    public void testWriteResultsWithTopClasses() {
        List<QuestionAnsweringInferenceResults.TopAnswerEntry> entries = Arrays.asList(
            new QuestionAnsweringInferenceResults.TopAnswerEntry("foo", 0.7, 0, 3),
            new QuestionAnsweringInferenceResults.TopAnswerEntry("bar", 0.2, 11, 14),
            new QuestionAnsweringInferenceResults.TopAnswerEntry("baz", 0.1, 4, 7)
        );
        QuestionAnsweringInferenceResults result = new QuestionAnsweringInferenceResults(
            "foo",
            0,
            3,
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
    protected QuestionAnsweringInferenceResults createTestInstance() {
        return createRandomResults();
    }

    @Override
    protected QuestionAnsweringInferenceResults mutateInstance(QuestionAnsweringInferenceResults instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<QuestionAnsweringInferenceResults> instanceReader() {
        return QuestionAnsweringInferenceResults::new;
    }

    @Override
    void assertFieldValues(QuestionAnsweringInferenceResults createdInstance, IngestDocument document, String resultsField) {
        String path = resultsField + "." + createdInstance.getResultsField();
        assertThat(document.getFieldValue(path, String.class), equalTo(createdInstance.predictedValue()));
    }
}
