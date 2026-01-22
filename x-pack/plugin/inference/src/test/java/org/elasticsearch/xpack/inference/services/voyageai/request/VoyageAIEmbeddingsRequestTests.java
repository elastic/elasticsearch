/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingType;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.VoyageAIEmbeddingsTaskSettings;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.voyageai.request.VoyageAIEmbeddingsRequestEntity.convertToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class VoyageAIEmbeddingsRequestTests extends ESTestCase {
    public void testCreateRequest_UrlDefined() throws IOException {
        var inputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var request = createRequest(
            List.of("abc"),
            inputType,
            VoyageAIEmbeddingsModelTests.createModel("url", "secret", VoyageAIEmbeddingsTaskSettings.EMPTY_SETTINGS, null, null, "model")
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(VoyageAIUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(VoyageAIUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertToString(inputType);
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("input", List.of("abc"), "model", "model", "output_dtype", "float", "input_type", convertedInputType))
            );
        } else {
            MatcherAssert.assertThat(requestMap, is(Map.of("input", List.of("abc"), "model", "model", "output_dtype", "float")));
        }
    }

    public void testCreateRequest_AllOptionsDefined() throws IOException {
        var inputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var request = createRequest(
            List.of("abc"),
            inputType,
            VoyageAIEmbeddingsModelTests.createModel("url", "secret", new VoyageAIEmbeddingsTaskSettings(null, null), null, null, "model")
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(VoyageAIUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(VoyageAIUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertToString(inputType);
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("input", List.of("abc"), "model", "model", "input_type", convertedInputType, "output_dtype", "float"))
            );
        } else {
            MatcherAssert.assertThat(requestMap, is(Map.of("input", List.of("abc"), "model", "model", "output_dtype", "float")));
        }

    }

    public void testCreateRequest_DimensionDefined() throws IOException {
        var inputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var request = createRequest(
            List.of("abc"),
            inputType,
            VoyageAIEmbeddingsModelTests.createModel(
                "url",
                "secret",
                new VoyageAIEmbeddingsTaskSettings(InputType.INGEST, null),
                null,
                2048,
                "model"
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(VoyageAIUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(VoyageAIUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertToString(inputType);
            MatcherAssert.assertThat(
                requestMap,
                is(
                    Map.of(
                        "input",
                        List.of("abc"),
                        "model",
                        "model",
                        "input_type",
                        convertedInputType,
                        "output_dtype",
                        "float",
                        "output_dimension",
                        2048
                    )
                )
            );
        } else {
            MatcherAssert.assertThat(
                requestMap,
                is(
                    Map.of(
                        "input",
                        List.of("abc"),
                        "model",
                        "model",
                        "input_type",
                        "document",
                        "output_dtype",
                        "float",
                        "output_dimension",
                        2048
                    )
                )
            );
        }
    }

    public void testCreateRequest_EmbeddingTypeDefined() throws IOException {
        var inputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var request = createRequest(
            List.of("abc"),
            inputType,
            VoyageAIEmbeddingsModelTests.createModel(
                "url",
                "secret",
                new VoyageAIEmbeddingsTaskSettings(InputType.INGEST, null),
                null,
                2048,
                "model",
                VoyageAIEmbeddingType.BYTE
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(VoyageAIUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(VoyageAIUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertToString(inputType);
            MatcherAssert.assertThat(
                requestMap,
                is(
                    Map.of(
                        "input",
                        List.of("abc"),
                        "model",
                        "model",
                        "input_type",
                        convertedInputType,
                        "output_dtype",
                        "int8",
                        "output_dimension",
                        2048
                    )
                )
            );
        } else {
            MatcherAssert.assertThat(
                requestMap,
                is(
                    Map.of(
                        "input",
                        List.of("abc"),
                        "model",
                        "model",
                        "input_type",
                        "document",
                        "output_dtype",
                        "int8",
                        "output_dimension",
                        2048
                    )
                )
            );
        }
    }

    public void testCreateRequest_TaskSettingsInputType() throws IOException {
        var inputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var request = createRequest(
            List.of("abc"),
            null,
            VoyageAIEmbeddingsModelTests.createModel(
                "url",
                "secret",
                new VoyageAIEmbeddingsTaskSettings(inputType, null),
                null,
                null,
                "model"
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(VoyageAIUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(VoyageAIUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        if (InputType.isSpecified(inputType)) {
            var convertedInputType = convertToString(inputType);
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("input", List.of("abc"), "model", "model", "input_type", convertedInputType, "output_dtype", "float"))
            );
        } else {
            MatcherAssert.assertThat(requestMap, is(Map.of("input", List.of("abc"), "model", "model", "output_dtype", "float")));
        }
    }

    public void testCreateRequest_RequestInputTypeTakesPrecedence() throws IOException {
        var requestInputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var taskSettingsInputType = InputTypeTests.randomSearchAndIngestWithNullWithoutUnspecified();
        var request = createRequest(
            List.of("abc"),
            requestInputType,
            VoyageAIEmbeddingsModelTests.createModel(
                "url",
                "secret",
                new VoyageAIEmbeddingsTaskSettings(taskSettingsInputType, null),
                null,
                null,
                "model"
            )
        );

        var httpRequest = request.createHttpRequest();
        MatcherAssert.assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        MatcherAssert.assertThat(httpPost.getURI().toString(), is("url"));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        MatcherAssert.assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        MatcherAssert.assertThat(
            httpPost.getLastHeader(VoyageAIUtils.REQUEST_SOURCE_HEADER).getValue(),
            is(VoyageAIUtils.ELASTIC_REQUEST_SOURCE)
        );

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        if (InputType.isSpecified(requestInputType)) {
            var convertedInputType = convertToString(requestInputType);
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("input", List.of("abc"), "model", "model", "input_type", convertedInputType, "output_dtype", "float"))
            );
        } else if (InputType.isSpecified(taskSettingsInputType)) {
            var convertedInputType = convertToString(taskSettingsInputType);
            MatcherAssert.assertThat(
                requestMap,
                is(Map.of("input", List.of("abc"), "model", "model", "input_type", convertedInputType, "output_dtype", "float"))
            );
        } else {
            MatcherAssert.assertThat(requestMap, is(Map.of("input", List.of("abc"), "model", "model", "output_dtype", "float")));
        }
    }

    public static VoyageAIEmbeddingsRequest createRequest(List<String> input, InputType inputType, VoyageAIEmbeddingsModel model) {
        return new VoyageAIEmbeddingsRequest(input, inputType, model);
    }
}
