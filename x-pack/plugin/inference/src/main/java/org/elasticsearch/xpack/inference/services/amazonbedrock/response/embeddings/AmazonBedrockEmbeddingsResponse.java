/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.response.embeddings;

import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.inference.external.response.XContentUtils;
import org.elasticsearch.xpack.inference.services.amazonbedrock.AmazonBedrockProvider;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.AmazonBedrockRequest;
import org.elasticsearch.xpack.inference.services.amazonbedrock.request.embeddings.AmazonBedrockEmbeddingsRequest;
import org.elasticsearch.xpack.inference.services.amazonbedrock.response.AmazonBedrockResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseList;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

public class AmazonBedrockEmbeddingsResponse extends AmazonBedrockResponse {
    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in Amazon Bedrock embeddings response";
    private final InvokeModelResponse result;

    public AmazonBedrockEmbeddingsResponse(InvokeModelResponse invokeModelResult) {
        this.result = invokeModelResult;
    }

    @Override
    public InferenceServiceResults accept(AmazonBedrockRequest request) {
        if (request instanceof AmazonBedrockEmbeddingsRequest asEmbeddingsRequest) {
            return fromResponse(result, asEmbeddingsRequest.provider());
        }

        throw new ElasticsearchException("unexpected request type [" + request.getClass() + "]");
    }

    public static TextEmbeddingFloatResults fromResponse(InvokeModelResponse response, AmazonBedrockProvider provider) {
        var charset = StandardCharsets.UTF_8;
        var bodyText = String.valueOf(charset.decode(response.body().asByteBuffer()));

        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, bodyText)) {
            // move to the first token
            jsonParser.nextToken();

            XContentParser.Token token = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            var embeddingList = parseEmbeddings(jsonParser, provider);

            return new TextEmbeddingFloatResults(embeddingList);
        } catch (IOException e) {
            throw new ElasticsearchException(e);
        }
    }

    private static List<TextEmbeddingFloatResults.Embedding> parseEmbeddings(XContentParser jsonParser, AmazonBedrockProvider provider)
        throws IOException {
        switch (provider) {
            case AMAZONTITAN -> {
                return parseTitanEmbeddings(jsonParser);
            }
            case COHERE -> {
                return parseCohereEmbeddings(jsonParser);
            }
            default -> throw new IOException("Unsupported provider [" + provider + "]");
        }
    }

    private static List<TextEmbeddingFloatResults.Embedding> parseTitanEmbeddings(XContentParser parser) throws IOException {
        /*
        Titan response:
        {
            "embedding": [float, float, ...],
            "inputTextTokenCount": int
        }
        */
        positionParserAtTokenAfterField(parser, "embedding", FAILED_TO_FIND_FIELD_TEMPLATE);
        List<Float> embeddingValuesList = parseList(parser, XContentUtils::parseFloat);
        var embeddingValues = TextEmbeddingFloatResults.Embedding.of(embeddingValuesList);
        return List.of(embeddingValues);
    }

    private static List<TextEmbeddingFloatResults.Embedding> parseCohereEmbeddings(XContentParser parser) throws IOException {
        /*
        Cohere response:
        {
            "embeddings": [
                [< array of 1024 floats >],
                ...
            ],
            "id": string,
            "response_type" : "embeddings_floats",
            "texts": [string]
        }
         */
        positionParserAtTokenAfterField(parser, "embeddings", FAILED_TO_FIND_FIELD_TEMPLATE);

        List<TextEmbeddingFloatResults.Embedding> embeddingList = parseList(
            parser,
            AmazonBedrockEmbeddingsResponse::parseCohereEmbeddingsListItem
        );

        return embeddingList;
    }

    private static TextEmbeddingFloatResults.Embedding parseCohereEmbeddingsListItem(XContentParser parser) throws IOException {
        List<Float> embeddingValuesList = parseList(parser, XContentUtils::parseFloat);
        return TextEmbeddingFloatResults.Embedding.of(embeddingValuesList);
    }

}
