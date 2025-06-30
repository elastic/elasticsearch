/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.response;

import org.apache.http.HttpResponse;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser.RERANK_PARSER_DOCUMENT_TEXT;
import static org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser.RERANK_PARSER_INDEX;
import static org.elasticsearch.xpack.inference.services.custom.response.RerankResponseParser.RERANK_PARSER_SCORE;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class RerankResponseParserTests extends AbstractBWCWireSerializationTestCase<RerankResponseParser> {

    public static RerankResponseParser createRandom() {
        var indexPath = randomBoolean() ? "$." + randomAlphaOfLength(5) : null;
        var documentTextPath = randomBoolean() ? "$." + randomAlphaOfLength(5) : null;
        return new RerankResponseParser("$." + randomAlphaOfLength(5), indexPath, documentTextPath);
    }

    public void testFromMap() {
        var validation = new ValidationException();
        var parser = RerankResponseParser.fromMap(
            new HashMap<>(
                Map.of(
                    RERANK_PARSER_SCORE,
                    "$.result.scores[*].score",
                    RERANK_PARSER_INDEX,
                    "$.result.scores[*].index",
                    RERANK_PARSER_DOCUMENT_TEXT,
                    "$.result.scores[*].document_text"
                )
            ),
            "scope",
            validation
        );

        assertThat(
            parser,
            is(new RerankResponseParser("$.result.scores[*].score", "$.result.scores[*].index", "$.result.scores[*].document_text"))
        );
    }

    public void testFromMap_WithoutOptionalFields() {
        var validation = new ValidationException();
        var parser = RerankResponseParser.fromMap(
            new HashMap<>(Map.of(RERANK_PARSER_SCORE, "$.result.scores[*].score")),
            "scope",
            validation
        );

        assertThat(parser, is(new RerankResponseParser("$.result.scores[*].score", null, null)));
    }

    public void testFromMap_ThrowsException_WhenRequiredFieldsAreNotPresent() {
        var validation = new ValidationException();
        var exception = expectThrows(
            ValidationException.class,
            () -> RerankResponseParser.fromMap(new HashMap<>(Map.of("not_path", "$.result[*].embeddings")), "scope", validation)
        );

        assertThat(
            exception.getMessage(),
            is("Validation Failed: 1: [scope.json_parser] does not contain the required setting [relevance_score];")
        );
    }

    public void testToXContent() throws IOException {
        var entity = new RerankResponseParser("$.result.scores[*].score", "$.result.scores[*].index", "$.result.scores[*].document_text");

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        {
            builder.startObject();
            entity.toXContent(builder, null);
            builder.endObject();
        }
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace("""
            {
                "json_parser": {
                    "relevance_score": "$.result.scores[*].score",
                    "reranked_index": "$.result.scores[*].index",
                    "document_text": "$.result.scores[*].document_text"
                }
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    public void testToXContent_WithoutOptionalFields() throws IOException {
        var entity = new RerankResponseParser("$.result.scores[*].score");

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        {
            builder.startObject();
            entity.toXContent(builder, null);
            builder.endObject();
        }
        String xContentResult = Strings.toString(builder);

        var expected = XContentHelper.stripWhitespace("""
            {
                "json_parser": {
                    "relevance_score": "$.result.scores[*].score"
                }
            }
            """);

        assertThat(xContentResult, is(expected));
    }

    public void testParse() throws IOException {
        String responseJson = """
            {
              "request_id": "450fcb80-f796-46c1-8d69-e1e86d29aa9f",
              "latency": 564.903929,
              "usage": {
                "doc_count": 2
              },
              "result": {
               "scores":[
                 {
                   "index":1,
                   "score": 1.37
                 },
                 {
                   "index":0,
                   "score": -0.3
                 }
               ]
              }
            }
            """;

        var parser = new RerankResponseParser("$.result.scores[*].score", "$.result.scores[*].index", null);
        RankedDocsResults parsedResults = (RankedDocsResults) parser.parse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults,
            is(
                new RankedDocsResults(
                    List.of(new RankedDocsResults.RankedDoc(1, 1.37f, null), new RankedDocsResults.RankedDoc(0, -0.3f, null))
                )
            )
        );
    }

    public void testParse_ThrowsException_WhenIndex_IsInvalid() {
        String responseJson = """
            {
              "request_id": "450fcb80-f796-46c1-8d69-e1e86d29aa9f",
              "latency": 564.903929,
              "usage": {
                "doc_count": 2
              },
              "result": {
               "scores":[
                 {
                   "index":"abc",
                   "score": 1.37
                 },
                 {
                   "index":0,
                   "score": -0.3
                 }
               ]
              }
            }
            """;

        var parser = new RerankResponseParser("$.result.scores[*].score", "$.result.scores[*].index", null);

        var exception = expectThrows(
            IllegalStateException.class,
            () -> parser.parse(new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8)))
        );

        assertThat(
            exception.getMessage(),
            is(
                "Failed to parse rerank indices, error: Failed to parse list entry [0], "
                    + "error: Unable to convert field [result.scores] of type [String] to Number"
            )
        );
    }

    public void testParse_ThrowsException_WhenScore_IsInvalid() {
        String responseJson = """
            {
              "request_id": "450fcb80-f796-46c1-8d69-e1e86d29aa9f",
              "latency": 564.903929,
              "usage": {
                "doc_count": 2
              },
              "result": {
               "scores":[
                 {
                   "index":1,
                   "score": true
                 },
                 {
                   "index":0,
                   "score": -0.3
                 }
               ]
              }
            }
            """;

        var parser = new RerankResponseParser("$.result.scores[*].score", "$.result.scores[*].index", null);

        var exception = expectThrows(
            IllegalStateException.class,
            () -> parser.parse(new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8)))
        );

        assertThat(
            exception.getMessage(),
            is(
                "Failed to parse rerank scores, error: Failed to parse list entry [0], "
                    + "error: Unable to convert field [result.scores] of type [Boolean] to Number"
            )
        );
    }

    public void testParse_ThrowsException_WhenDocument_IsInvalid() {
        String responseJson = """
            {
              "request_id": "450fcb80-f796-46c1-8d69-e1e86d29aa9f",
              "latency": 564.903929,
              "usage": {
                "doc_count": 2
              },
              "result": {
               "scores":[
                 {
                   "index":1,
                   "score": 0.2,
                   "document": 1
                 },
                 {
                   "index":0,
                   "score": -0.3,
                   "document": "a document"
                 }
               ]
              }
            }
            """;

        var parser = new RerankResponseParser("$.result.scores[*].score", "$.result.scores[*].index", "$.result.scores[*].document");

        var exception = expectThrows(
            IllegalStateException.class,
            () -> parser.parse(new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8)))
        );

        assertThat(
            exception.getMessage(),
            is(
                "Failed to parse rerank documents, error: Failed to parse list entry [0], error: "
                    + "Unable to convert field [result.scores] of type [Integer] to [String]"
            )
        );
    }

    public void testParse_ThrowsException_WhenIndices_ListSizeDoesNotMatchScores() {
        String responseJson = """
            {
              "request_id": "450fcb80-f796-46c1-8d69-e1e86d29aa9f",
              "latency": 564.903929,
              "usage": {
                "doc_count": 2
              },
              "result": {
                "indices": [1],
                "scores": [0.2, 0.3],
                "documents": ["a", "b"]
              }
            }
            """;

        var parser = new RerankResponseParser("$.result.scores", "$.result.indices", "$.result.documents");

        var exception = expectThrows(
            IllegalStateException.class,
            () -> parser.parse(new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8)))
        );

        assertThat(exception.getMessage(), is("The number of index fields [1] was not the same as the number of scores [2]"));
    }

    public void testParse_ThrowsException_WhenDocuments_ListSizeDoesNotMatchScores() {
        String responseJson = """
            {
              "request_id": "450fcb80-f796-46c1-8d69-e1e86d29aa9f",
              "latency": 564.903929,
              "usage": {
                "doc_count": 2
              },
              "result": {
                "indices": [1, 0],
                "scores": [0.2, 0.3],
                "documents": ["a"]
              }
            }
            """;

        var parser = new RerankResponseParser("$.result.scores", "$.result.indices", "$.result.documents");

        var exception = expectThrows(
            IllegalStateException.class,
            () -> parser.parse(new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8)))
        );

        assertThat(exception.getMessage(), is("The number of document fields [1] was not the same as the number of scores [2]"));
    }

    public void testParse_WithoutIndex() throws IOException {
        String responseJson = """
            {
              "request_id": "450fcb80-f796-46c1-8d69-e1e86d29aa9f",
              "latency": 564.903929,
              "usage": {
                "doc_count": 2
              },
              "result": {
               "scores":[
                 {
                   "score": 1.37
                 },
                 {
                   "score": -0.3
                 }
               ]
              }
            }
            """;

        var parser = new RerankResponseParser("$.result.scores[*].score", null, null);
        RankedDocsResults parsedResults = (RankedDocsResults) parser.parse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults,
            is(
                new RankedDocsResults(
                    List.of(new RankedDocsResults.RankedDoc(0, 1.37f, null), new RankedDocsResults.RankedDoc(1, -0.3f, null))
                )
            )
        );
    }

    public void testParse_CohereResponseFormat() throws IOException {
        String responseJson = """
            {
                "index": "44873262-1315-4c06-8433-fdc90c9790d0",
                "results": [
                    {
                        "document": {
                            "text": "Washington, D.C.."
                        },
                        "index": 2,
                        "relevance_score": 0.98005307
                    },
                    {
                        "document": {
                            "text": "Capital punishment has existed in the United States since beforethe United States was a country. "
                        },
                        "index": 3,
                        "relevance_score": 0.27904198
                    },
                    {
                        "document": {
                            "text": "Carson City is the capital city of the American state of Nevada."
                        },
                        "index": 0,
                        "relevance_score": 0.10194652
                    }
                ],
                "meta": {
                    "api_version": {
                        "version": "1"
                    },
                    "billed_units": {
                        "search_units": 1
                    }
                }
            }
            """;

        var parser = new RerankResponseParser("$.results[*].relevance_score", "$.results[*].index", "$.results[*].document.text");
        RankedDocsResults parsedResults = (RankedDocsResults) parser.parse(
            new HttpResult(mock(HttpResponse.class), responseJson.getBytes(StandardCharsets.UTF_8))
        );

        assertThat(
            parsedResults,
            is(
                new RankedDocsResults(
                    List.of(
                        new RankedDocsResults.RankedDoc(2, 0.98005307f, "Washington, D.C.."),
                        new RankedDocsResults.RankedDoc(
                            3,
                            0.27904198f,
                            "Capital punishment has existed in the United States since beforethe United States was a country. "
                        ),
                        new RankedDocsResults.RankedDoc(0, 0.10194652f, "Carson City is the capital city of the American state of Nevada.")
                    )
                )
            )
        );
    }

    @Override
    protected RerankResponseParser mutateInstanceForVersion(RerankResponseParser instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected Writeable.Reader<RerankResponseParser> instanceReader() {
        return RerankResponseParser::new;
    }

    @Override
    protected RerankResponseParser createTestInstance() {
        return createRandom();
    }

    @Override
    protected RerankResponseParser mutateInstance(RerankResponseParser instance) throws IOException {
        return randomValueOtherThan(instance, RerankResponseParserTests::createRandom);
    }
}
