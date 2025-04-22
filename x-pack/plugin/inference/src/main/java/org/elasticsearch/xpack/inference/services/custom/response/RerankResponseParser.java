/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.custom.response;

import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.common.MapPathExtractor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractRequiredString;
import static org.elasticsearch.xpack.inference.services.custom.CustomServiceSettings.JSON_PARSER;

public class RerankResponseParser extends BaseCustomResponseParser<RankedDocsResults> {

    public static final String NAME = "rerank_response_parser";
    public static final String RERANK_PARSER_SCORE = "relevance_score";
    public static final String RERANK_PARSER_INDEX = "reranked_index";
    public static final String RERANK_PARSER_DOCUMENT_TEXT = "document_text";

    private final String relevanceScorePath;
    private final String rerankIndexPath;
    private final String documentTextPath;

    public static RerankResponseParser fromMap(Map<String, Object> responseParserMap, ValidationException validationException) {

        var relevanceScore = extractRequiredString(responseParserMap, RERANK_PARSER_SCORE, JSON_PARSER, validationException);
        var rerankIndex = extractOptionalString(responseParserMap, RERANK_PARSER_INDEX, JSON_PARSER, validationException);
        var documentText = extractOptionalString(responseParserMap, RERANK_PARSER_DOCUMENT_TEXT, JSON_PARSER, validationException);

        if (relevanceScore == null) {
            throw validationException;
        }

        return new RerankResponseParser(relevanceScore, rerankIndex, documentText);
    }

    public RerankResponseParser(String relevanceScorePath) {
        this(relevanceScorePath, null, null);
    }

    public RerankResponseParser(String relevanceScorePath, @Nullable String rerankIndexPath, @Nullable String documentTextPath) {
        this.relevanceScorePath = Objects.requireNonNull(relevanceScorePath);
        this.rerankIndexPath = rerankIndexPath;
        this.documentTextPath = documentTextPath;
    }

    public RerankResponseParser(StreamInput in) throws IOException {
        this.relevanceScorePath = in.readString();
        this.rerankIndexPath = in.readOptionalString();
        this.documentTextPath = in.readOptionalString();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(relevanceScorePath);
        out.writeOptionalString(rerankIndexPath);
        out.writeOptionalString(documentTextPath);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(JSON_PARSER);
        {
            builder.field(RERANK_PARSER_SCORE, relevanceScorePath);
            if (rerankIndexPath != null) {
                builder.field(RERANK_PARSER_INDEX, rerankIndexPath);
            }

            if (documentTextPath != null) {
                builder.field(RERANK_PARSER_DOCUMENT_TEXT, documentTextPath);
            }
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RerankResponseParser that = (RerankResponseParser) o;
        return Objects.equals(relevanceScorePath, that.relevanceScorePath)
            && Objects.equals(rerankIndexPath, that.rerankIndexPath)
            && Objects.equals(documentTextPath, that.documentTextPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(relevanceScorePath, rerankIndexPath, documentTextPath);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public RankedDocsResults transform(Map<String, Object> map) {
        var scores = convertToListOfFloats(MapPathExtractor.extract(map, relevanceScorePath));

        List<Integer> indices = null;
        if (rerankIndexPath != null) {
            indices = convertToListOfIntegers(MapPathExtractor.extract(map, rerankIndexPath));
        }

        List<String> documents = null;
        if (documentTextPath != null) {
            documents = validateAndCastList(
                validateList(MapPathExtractor.extract(map, documentTextPath)),
                (obj) -> toType(obj, String.class)
            );
        }

        if (indices != null && indices.size() != scores.size()) {
            throw new IllegalStateException(
                Strings.format(
                    "The number of index paths [%d] was not the same as the number of scores [%d]",
                    indices.size(),
                    scores.size()
                )
            );
        }

        if (documents != null && documents.size() != scores.size()) {
            throw new IllegalStateException(
                Strings.format(
                    "The number of document texts [%d] was no the same as the number of scores [%d]",
                    documents.size(),
                    scores.size()
                )
            );
        }

        var rankedDocs = new ArrayList<RankedDocsResults.RankedDoc>();
        for (int i = 0; i < scores.size(); i++) {
            var index = indices != null ? indices.get(i) : i;
            var score = scores.get(i);
            var document = documents != null ? documents.get(i) : null;
            rankedDocs.add(new RankedDocsResults.RankedDoc(index, score, document));
        }

        return new RankedDocsResults(rankedDocs);
    }
}
