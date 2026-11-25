/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.response;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class OciGenAiRerankResponseEntity {

    private static final String DOCUMENT_RANKS_FIELD = "documentRanks";
    private static final String INDEX_FIELD = "index";
    private static final String RELEVANCE_SCORE_FIELD = "relevanceScore";
    private static final String DOCUMENT_FIELD = "document";

    private record DocumentRank(int index, float relevanceScore, @Nullable String document) {}

    private static final ConstructingObjectParser<DocumentRank, Void> DOCUMENT_RANK_PARSER = new ConstructingObjectParser<>(
        "oci_genai_document_rank",
        true,
        args -> new DocumentRank((Integer) args[0], (Float) args[1], (String) args[2])
    );

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<List<DocumentRank>, Void> RESPONSE_PARSER = new ConstructingObjectParser<>(
        "oci_genai_rerank_response",
        true,
        args -> (List<DocumentRank>) args[0]
    );

    static {
        DOCUMENT_RANK_PARSER.declareInt(constructorArg(), new ParseField(INDEX_FIELD));
        DOCUMENT_RANK_PARSER.declareFloat(constructorArg(), new ParseField(RELEVANCE_SCORE_FIELD));
        DOCUMENT_RANK_PARSER.declareString(optionalConstructorArg(), new ParseField(DOCUMENT_FIELD));
        RESPONSE_PARSER.declareObjectArray(constructorArg(), DOCUMENT_RANK_PARSER, new ParseField(DOCUMENT_RANKS_FIELD));
    }

    /**
     * Parses the OCI Generative AI {@code rerankText} response, which looks like:
     *
     * <pre>
     *     <code>
     * {
     *   "documentRanks": [
     *     { "document": "Paris is the capital of France.", "index": 0, "relevanceScore": 0.98 },
     *     { "document": "Berlin is in Germany.", "index": 2, "relevanceScore": 0.12 }
     *   ],
     *   "id": "...",
     *   "modelId": "cohere.rerank-v3.5",
     *   "modelVersion": "3.5"
     * }
     *     </code>
     * </pre>
     *
     * The {@code document} field is only present when the request asked for the documents to be echoed.
     */
    public static RankedDocsResults fromResponse(HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            var documentRanks = RESPONSE_PARSER.apply(parser, null);
            return new RankedDocsResults(
                documentRanks.stream()
                    .map(rank -> new RankedDocsResults.RankedDoc(rank.index(), rank.relevanceScore(), rank.document()))
                    .toList()
            );
        }
    }

    private OciGenAiRerankResponseEntity() {}
}
