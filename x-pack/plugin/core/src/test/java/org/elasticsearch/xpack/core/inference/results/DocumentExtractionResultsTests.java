/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class DocumentExtractionResultsTests extends AbstractWireSerializingTestCase<DocumentExtractionResults> {

    public void testToXContent_CreatesTheRightFormatForASingleResult() {
        var result = new DocumentExtractionResults(
            List.of(new DocumentExtractionResults.Result("# Annual Report 2025", "markdown", Map.of("title", "Annual Report 2025")))
        );

        assertThat(
            result.asMap(),
            is(
                Map.of(
                    DocumentExtractionResults.DOCUMENT_EXTRACTION,
                    List.of(
                        Map.of(
                            DocumentExtractionResults.Result.CONTENT,
                            "# Annual Report 2025",
                            DocumentExtractionResults.Result.FORMAT,
                            "markdown",
                            DocumentExtractionResults.Result.METADATA,
                            Map.of("title", "Annual Report 2025")
                        )
                    )
                )
            )
        );

        String xContentResult = Strings.toTruncatedString(result, true, true);
        assertThat(xContentResult, is("""
            {
              "document_extraction" : [
                {
                  "content" : "# Annual Report 2025",
                  "format" : "markdown",
                  "metadata" : {
                    "title" : "Annual Report 2025"
                  }
                }
              ]
            }"""));
    }

    public void testToXContent_WithoutMetadata_OmitsMetadataField() {
        var result = new DocumentExtractionResults(List.of(new DocumentExtractionResults.Result("some content", "text", null)));

        assertThat(
            result.asMap(),
            is(
                Map.of(
                    DocumentExtractionResults.DOCUMENT_EXTRACTION,
                    List.of(
                        Map.of(DocumentExtractionResults.Result.CONTENT, "some content", DocumentExtractionResults.Result.FORMAT, "text")
                    )
                )
            )
        );

        String xContentResult = Strings.toTruncatedString(result, true, true);
        assertThat(xContentResult, is("""
            {
              "document_extraction" : [
                {
                  "content" : "some content",
                  "format" : "text"
                }
              ]
            }"""));
    }

    public void testToXContent_CreatesTheRightFormatForMultipleResults() {
        var results = new DocumentExtractionResults(
            List.of(
                new DocumentExtractionResults.Result("content 1", "markdown", Map.of()),
                new DocumentExtractionResults.Result("content 2", "markdown", Map.of())
            )
        );

        String xContentResult = Strings.toTruncatedString(results, true, true);
        assertThat(xContentResult, is("""
            {
              "document_extraction" : [
                {
                  "content" : "content 1",
                  "format" : "markdown"
                },
                {
                  "content" : "content 2",
                  "format" : "markdown"
                }
              ]
            }"""));
    }

    public void testResult_PredictedValue_ReturnsContent() {
        var result = new DocumentExtractionResults.Result("some content", "markdown", Map.of());
        assertThat(result.predictedValue(), is("some content"));
    }

    @Override
    protected Writeable.Reader<DocumentExtractionResults> instanceReader() {
        return DocumentExtractionResults::new;
    }

    @Override
    protected DocumentExtractionResults createTestInstance() {
        return createRandomResults();
    }

    public static DocumentExtractionResults createRandomResults() {
        return new DocumentExtractionResults(randomList(0, 10, DocumentExtractionResultsTests::createRandomResult));
    }

    private static DocumentExtractionResults.Result createRandomResult() {
        return new DocumentExtractionResults.Result(
            randomAlphaOfLengthBetween(1, 100),
            randomFrom("markdown", "text", "html"),
            randomBoolean() ? Map.of() : Map.of(randomAlphanumericOfLength(8), randomAlphanumericOfLength(8))
        );
    }

    @Override
    protected DocumentExtractionResults mutateInstance(DocumentExtractionResults instance) throws IOException {
        var results = new ArrayList<>(instance.results());
        results.add(createRandomResult());
        return new DocumentExtractionResults(results);
    }
}
