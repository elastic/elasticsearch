/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredMap;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.throwIfNotEmptyMap;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.COMPLETION_PARSER_RESULT;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.JSON_PARSER;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.RERANK_PARSER_DOCUMENT_TEXT;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.RERANK_PARSER_INDEX;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.RERANK_PARSER_SCORE;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.SPARSE_EMBEDDING_PARSER_TOKEN;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.SPARSE_EMBEDDING_PARSER_WEIGHT;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.SPARSE_EMBEDDING_RESULT;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.SPARSE_RESULT_PATH;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.SPARSE_RESULT_VALUE;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.TEXT_EMBEDDING_PARSER_EMBEDDINGS;

public class ResponseJsonParser {
    public static final String FIELD_NAME = JSON_PARSER;

    private String taskTypeStr;

    private String textEmbeddingsPath;

    private String sparseResultPath;
    private String sparseTokenPath;
    private String sparseWeightPath;

    private String rerankedIndexPath;
    private String relevanceScorePath;
    private String documentTextPath;

    private String completionResultPath;

    public ResponseJsonParser(TaskType taskType, Map<String, Object> responseParserMap, ValidationException validationException) {
        this.taskTypeStr = taskType.toString();
        switch (taskType) {
            case TEXT_EMBEDDING -> textEmbeddingsPath = extractRequiredString(
                responseParserMap,
                TEXT_EMBEDDING_PARSER_EMBEDDINGS,
                JSON_PARSER,
                validationException
            );
            case SPARSE_EMBEDDING -> {
                Map<String, Object> sparseResultMap = extractRequiredMap(
                    responseParserMap,
                    SPARSE_EMBEDDING_RESULT,
                    JSON_PARSER,
                    validationException
                );
                if (sparseResultMap == null) {
                    throw validationException;
                }
                sparseResultPath = extractRequiredString(sparseResultMap, SPARSE_RESULT_PATH, JSON_PARSER, validationException);
                Map<String, Object> sparseResultValueMap = extractRequiredMap(
                    sparseResultMap,
                    SPARSE_RESULT_VALUE,
                    JSON_PARSER,
                    validationException
                );
                if (sparseResultValueMap == null) {
                    throw validationException;
                }
                sparseTokenPath = extractRequiredString(
                    sparseResultValueMap,
                    SPARSE_EMBEDDING_PARSER_TOKEN,
                    JSON_PARSER,
                    validationException
                );
                sparseWeightPath = extractRequiredString(
                    sparseResultValueMap,
                    SPARSE_EMBEDDING_PARSER_WEIGHT,
                    JSON_PARSER,
                    validationException
                );
            }
            case RERANK -> {
                rerankedIndexPath = extractOptionalString(responseParserMap, RERANK_PARSER_INDEX, JSON_PARSER, validationException);

                relevanceScorePath = extractRequiredString(responseParserMap, RERANK_PARSER_SCORE, JSON_PARSER, validationException);

                documentTextPath = extractOptionalString(responseParserMap, RERANK_PARSER_DOCUMENT_TEXT, JSON_PARSER, validationException);
            }
            case COMPLETION -> completionResultPath = extractRequiredString(
                responseParserMap,
                COMPLETION_PARSER_RESULT,
                JSON_PARSER,
                validationException
            );
            default -> throw new IllegalArgumentException(
                String.format(Locale.ROOT, "json parser does not support taskType [%s]", taskType)
            );
        }
    }

    public ResponseJsonParser(StreamInput in) throws IOException {
        this.taskTypeStr = in.readString();
        TaskType taskType = TaskType.fromString(this.taskTypeStr);
        switch (taskType) {
            case TEXT_EMBEDDING -> this.textEmbeddingsPath = in.readString();
            case SPARSE_EMBEDDING -> {
                this.sparseResultPath = in.readString();
                this.sparseTokenPath = in.readString();
                this.sparseWeightPath = in.readString();
            }
            case RERANK -> {
                this.rerankedIndexPath = in.readOptionalString();
                this.relevanceScorePath = in.readString();
                this.documentTextPath = in.readOptionalString();
            }
            case COMPLETION -> this.completionResultPath = in.readString();
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(this.taskTypeStr);
        TaskType taskType = TaskType.fromString(this.taskTypeStr);
        switch (taskType) {
            case TEXT_EMBEDDING -> out.writeString(this.textEmbeddingsPath);
            case SPARSE_EMBEDDING -> {
                out.writeString(this.sparseResultPath);
                out.writeString(this.sparseTokenPath);
                out.writeString(this.sparseWeightPath);
            }
            case RERANK -> {
                out.writeOptionalString(this.rerankedIndexPath);
                out.writeString(this.relevanceScorePath);
                out.writeOptionalString(this.documentTextPath);
            }
            case COMPLETION -> out.writeString(this.completionResultPath);
        }
    }

    public String getTextEmbeddingsPath() {
        return textEmbeddingsPath;
    }

    public String getSparseResultPath() {
        return sparseResultPath;
    }

    public String getSparseTokenPath() {
        return sparseTokenPath;
    }

    public String getSparseWeightPath() {
        return sparseWeightPath;
    }

    public String getRerankedIndexPath() {
        return rerankedIndexPath;
    }

    public String getRelevanceScorePath() {
        return relevanceScorePath;
    }

    public String getDocumentTextPath() {
        return documentTextPath;
    }

    public String getCompletionResultPath() {
        return completionResultPath;
    }

    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(FIELD_NAME);
        {
            switch (TaskType.fromString(this.taskTypeStr)) {
                case TEXT_EMBEDDING -> builder.field(TEXT_EMBEDDING_PARSER_EMBEDDINGS, textEmbeddingsPath);
                case SPARSE_EMBEDDING -> {
                    builder.startObject(SPARSE_EMBEDDING_RESULT);
                    {
                        builder.field(SPARSE_RESULT_PATH, sparseResultPath);
                        builder.startObject(SPARSE_RESULT_VALUE);
                        {
                            builder.field(SPARSE_EMBEDDING_PARSER_TOKEN, sparseTokenPath);
                            builder.field(SPARSE_EMBEDDING_PARSER_WEIGHT, sparseWeightPath);
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                case RERANK -> {
                    if (rerankedIndexPath != null) {
                        builder.field(RERANK_PARSER_INDEX, rerankedIndexPath);
                    }
                    builder.field(RERANK_PARSER_SCORE, relevanceScorePath);
                    if (documentTextPath != null) {
                        builder.field(RERANK_PARSER_DOCUMENT_TEXT, documentTextPath);
                    }
                }
                case COMPLETION -> builder.field(COMPLETION_PARSER_RESULT, completionResultPath);
            }
        }
        builder.endObject();
        return builder;
    }

    public static ResponseJsonParser of(
        Map<String, Object> map,
        ValidationException validationException,
        String serviceName,
        ConfigurationParseContext context
    ) {
        Map<String, Object> responseParserMap = extractRequiredMap(map, FIELD_NAME, JSON_PARSER, validationException);
        if (responseParserMap == null) {
            throw validationException;
        }
        String taskTypeStr = extractRequiredString(responseParserMap, TaskType.NAME, FIELD_NAME, validationException);
        if (taskTypeStr == null) {
            throw validationException;
        }
        TaskType taskType = TaskType.fromString(taskTypeStr);
        ResponseJsonParser responseJsonParser = new ResponseJsonParser(taskType, responseParserMap, validationException);

        if (ConfigurationParseContext.isRequestContext(context)) {
            throwIfNotEmptyMap(responseParserMap, serviceName);
        }
        return responseJsonParser;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResponseJsonParser that = (ResponseJsonParser) o;
        return Objects.equals(taskTypeStr, that.taskTypeStr)
            && Objects.equals(textEmbeddingsPath, that.textEmbeddingsPath)
            && Objects.equals(sparseResultPath, that.sparseResultPath)
            && Objects.equals(sparseTokenPath, that.sparseTokenPath)
            && Objects.equals(sparseWeightPath, that.sparseWeightPath)
            && Objects.equals(rerankedIndexPath, that.rerankedIndexPath)
            && Objects.equals(relevanceScorePath, that.relevanceScorePath)
            && Objects.equals(documentTextPath, that.documentTextPath)
            && Objects.equals(completionResultPath, that.completionResultPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            taskTypeStr,
            textEmbeddingsPath,
            sparseResultPath,
            sparseTokenPath,
            sparseWeightPath,
            rerankedIndexPath,
            relevanceScorePath,
            documentTextPath,
            completionResultPath
        );
    }
}
