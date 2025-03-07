/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.custom;

import net.minidev.json.JSONArray;

import com.jayway.jsonpath.JsonPath;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.core.inference.results.CustomServiceResults;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResults;
import org.elasticsearch.xpack.core.ml.search.WeightedToken;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.external.request.custom.CustomRequest;
import org.elasticsearch.xpack.inference.services.custom.ResponseJsonParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public class CustomResponseEntity {
    private static final Logger logger = LogManager.getLogger(CustomResponseEntity.class);

    public static InferenceServiceResults fromResponse(Request request, HttpResult response) throws IOException {
        CustomRequest customRequest = (CustomRequest) request;
        String serviceType = customRequest.getServiceSettings().getServiceType();
        TaskType taskType = TaskType.fromStringOrStatusException(serviceType);
        ResponseJsonParser responseJsonParser = customRequest.getServiceSettings().getResponseJsonParser();

        InferenceServiceResults result = switch (taskType) {
            case TEXT_EMBEDDING -> fromTextEmbeddingResponse(response, responseJsonParser);
            case SPARSE_EMBEDDING -> fromSparseEmbeddingResponse(response, responseJsonParser);
            case RERANK -> fromRerankResponse(response, responseJsonParser);
            case COMPLETION -> fromCompletionResponse(response, responseJsonParser);
            case CUSTOM -> fromCustomResponse(response);
            default -> throw new ElasticsearchStatusException(unsupportedTaskTypeErrorMsg(taskType), RestStatus.BAD_REQUEST);
        };

        logger.debug(
            "Ai Search uri [{}] response: client cost [{}ms]",
            request.getURI() != null ? request.getURI() : "",
            System.currentTimeMillis() - customRequest.getStartTime()
        );

        return result;
    }

    private static CustomServiceResults fromCustomResponse(HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            return new CustomServiceResults(jsonParser.map());
        }
    }

    private static InferenceServiceResults fromTextEmbeddingResponse(HttpResult response, ResponseJsonParser responseJsonParser) {
        String embeddingPath = responseJsonParser.getTextEmbeddingsPath();

        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            JSONArray embeddingResults = JsonPath.read(jsonParser.map(), embeddingPath);
            if (embeddingResults == null || embeddingResults.isEmpty()) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "can't parse text_embeddings results from response, please check the path [%s]",
                        embeddingPath
                    )
                );
            }

            List<TextEmbeddingFloatResults.Embedding> embeddings = embeddingResults.stream()
                .map(
                    embeddingResult -> ((List<?>) embeddingResult).stream()
                        .map(obj -> ((Number) obj).floatValue())
                        .collect(Collectors.toList())
                )
                .map(TextEmbeddingFloatResults.Embedding::of)
                .collect(Collectors.toList());

            return new TextEmbeddingFloatResults(embeddings);
        } catch (Exception e) {
            logger.error("failed to parse text_embeddings results from response:", e);
            throw new IllegalArgumentException("failed to parse text_embeddings results from response:", e);
        }
    }

    private static InferenceServiceResults fromSparseEmbeddingResponse(HttpResult response, ResponseJsonParser responseJsonParser) {
        String resultPath = responseJsonParser.getSparseResultPath();
        String tokenPath = responseJsonParser.getSparseTokenPath();
        String weightPath = responseJsonParser.getSparseWeightPath();

        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            Object jsonObject = jsonParser.map();
            JSONArray sparseResults = JsonPath.read(jsonObject, resultPath);
            if (sparseResults == null || sparseResults.isEmpty()) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "can't parse sparse_result results from response, please check the path [%s]", resultPath)
                );
            }

            List<SparseEmbeddingResults.Embedding> embeddingList = new ArrayList<>();

            for (Object obj : sparseResults) {
                JSONArray tokenResults = JsonPath.read(obj, tokenPath);
                if (tokenResults == null || tokenResults.isEmpty()) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "can't parse sparse embeddings results from response, please check the path [%s]",
                            tokenPath
                        )
                    );
                }
                JSONArray weightResults = JsonPath.read(obj, weightPath);
                if (weightResults == null || weightResults.isEmpty()) {
                    throw new IllegalArgumentException(
                        String.format(
                            Locale.ROOT,
                            "can't parse sparse embeddings results from response, please check the path [%s]",
                            weightPath
                        )
                    );
                }
                List<String> tokens = tokenResults.stream().map(Object::toString).toList();
                List<Float> weights = weightResults.stream().map(weight -> ((Number) weight).floatValue()).toList();

                List<WeightedToken> weightedTokens = new ArrayList<>();
                if (tokens.size() != weights.size()) {
                    throw new IllegalArgumentException("Tokens and weights size does not match");
                }
                for (int i = 0; i < tokens.size(); i++) {
                    weightedTokens.add(new WeightedToken(tokens.get(i), weights.get(i)));
                }

                embeddingList.add(new SparseEmbeddingResults.Embedding(weightedTokens, false));
            }

            return new SparseEmbeddingResults(embeddingList);
        } catch (Exception e) {
            logger.error("failed to parse sparse_result results from response:", e);
            throw new IllegalArgumentException("failed to parse sparse_result results from response:", e);
        }
    }

    private static InferenceServiceResults fromRerankResponse(HttpResult response, ResponseJsonParser responseJsonParser) {
        String indexPath = responseJsonParser.getRerankedIndexPath();
        String scorePath = responseJsonParser.getRelevanceScorePath();
        String docPath = responseJsonParser.getDocumentTextPath();

        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            Object jsonObject = jsonParser.map();
            JSONArray indexResults = indexPath != null ? JsonPath.read(jsonObject, indexPath) : null;
            if (indexPath != null && (indexResults == null || indexResults.isEmpty())) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "can't parse reranked_index results from response, please check the path [%s]", indexPath)
                );
            }
            JSONArray scoreResults = JsonPath.read(jsonObject, scorePath);
            if (scoreResults == null || scoreResults.isEmpty()) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "can't parse relevance_score results from response, please check the path [%s]", scorePath)
                );
            }
            JSONArray docResults = docPath != null ? JsonPath.read(jsonObject, docPath) : null;
            if (docPath != null && (docResults == null || docResults.isEmpty())) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "can't parse doc results from response, please check the path [%s]", docPath)
                );
            }

            List<Integer> indices = indexResults != null ? indexResults.stream().map(index -> (Integer) index).toList() : null;

            List<Float> scores = scoreResults.stream().map(score -> ((Number) score).floatValue()).toList();

            List<String> docs = docResults != null ? docResults.stream().map(doc -> (String) doc).toList() : null;

            List<RankedDocsResults.RankedDoc> rerankResults = new ArrayList<>();
            if (indices != null && indices.size() != scores.size()) {
                throw new IllegalArgumentException("Indices and scores size does not match");
            }
            for (int i = 0; i < scores.size(); i++) {
                if (docs != null) {
                    if (indices != null) {
                        rerankResults.add(new RankedDocsResults.RankedDoc(indices.get(i), scores.get(i), docs.get(i)));
                    } else {
                        rerankResults.add(new RankedDocsResults.RankedDoc(i, scores.get(i), docs.get(i)));
                    }
                } else {
                    if (indices != null) {
                        rerankResults.add(new RankedDocsResults.RankedDoc(indices.get(i), scores.get(i), null));
                    } else {
                        rerankResults.add(new RankedDocsResults.RankedDoc(i, scores.get(i), null));
                    }
                }
            }

            return new RankedDocsResults(rerankResults);
        } catch (Exception e) {
            logger.error("failed to parse rerank results from response:", e);
            throw new IllegalArgumentException("failed to parse rerank results from response:", e);
        }
    }

    private static InferenceServiceResults fromCompletionResponse(HttpResult response, ResponseJsonParser responseJsonParser) {
        String resultPath = responseJsonParser.getCompletionResultPath();

        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            Object results = JsonPath.read(jsonParser.map(), resultPath);
            if (results == null) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "can't parse completion results from response, please check the path [%s]", resultPath)
                );
            }
            if (results instanceof String) {
                return new ChatCompletionResults(List.of(new ChatCompletionResults.Result((String) results)));
            } else if (results instanceof JSONArray jsonArrayResults) {
                if (jsonArrayResults.isEmpty()) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "can't parse completion results from response, please check the path [%s]", resultPath)
                    );
                }
                List<ChatCompletionResults.Result> completionResults = jsonArrayResults.stream()
                    .map(obj -> (String) obj)
                    .map(ChatCompletionResults.Result::new)
                    .collect(Collectors.toList());
                return new ChatCompletionResults(completionResults);
            } else {
                throw new IllegalArgumentException("Unsupported completion result type: " + results.getClass().getName());
            }
        } catch (Exception e) {
            logger.error("failed to parse completion results from response:", e);
            throw new IllegalArgumentException("failed to parse completion results from response:", e);
        }
    }

    public static String unsupportedTaskTypeErrorMsg(TaskType taskType) {
        return "ai search custom service does not support task type [" + taskType + "]";
    }
}
