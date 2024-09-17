/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.googlevertexai;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.parseList;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

public class GoogleVertexAiRerankResponseEntity {

    private static final String FAILED_TO_FIND_FIELD_TEMPLATE = "Failed to find required field [%s] in Google Vertex AI rerank response";

    /**
     * Parses the Google Vertex AI rerank response.
     *
     * For a request like:
     *
     * <pre>
     *     <code>
     *         {
     *              "query": "some query",
     *              "records": [
     *                  {
     *                      "id": "1",
     *                      "title": "title 1",
     *                      "content": "content 1"
     *                  },
     *                  {
     *                      "id": "2",
     *                      "title": "title 2",
     *                      "content": "content 2"
     *                  }
     *     ]
     * }
     *     </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     *     <code>
     *         {
     *              "records": [
     *                  {
     *                      "id": "2",
     *                      "title": "title 2",
     *                      "content": "content 2",
     *                      "score": 0.97
     *                  },
     *                  {
     *                      "id": "1",
     *                      "title": "title 1",
     *                      "content": "content 1",
     *                      "score": 0.18
     *                  }
     *             ]
     *         }
     *     </code>
     * </pre>
     */

    public static RankedDocsResults fromResponse(HttpResult response) throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);

        try (XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(jsonParser);

            XContentParser.Token token = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.START_OBJECT, token, jsonParser);

            positionParserAtTokenAfterField(jsonParser, "records", FAILED_TO_FIND_FIELD_TEMPLATE);

            var rankedDocs = doParse(jsonParser);

            return new RankedDocsResults(rankedDocs);
        }
    }

    private static List<RankedDocsResults.RankedDoc> doParse(XContentParser parser) throws IOException {
        return parseList(parser, (listParser, index) -> {
            var parsedRankedDoc = RankedDoc.parse(parser);

            if (parsedRankedDoc.content == null) {
                throw new IllegalStateException(format(FAILED_TO_FIND_FIELD_TEMPLATE, RankedDoc.CONTENT.getPreferredName()));
            }

            if (parsedRankedDoc.score == null) {
                throw new IllegalStateException(format(FAILED_TO_FIND_FIELD_TEMPLATE, RankedDoc.SCORE.getPreferredName()));
            }

            return new RankedDocsResults.RankedDoc(index, parsedRankedDoc.score, parsedRankedDoc.content);
        });
    }

    private record RankedDoc(@Nullable Float score, @Nullable String content) {

        private static final ParseField CONTENT = new ParseField("content");
        private static final ParseField SCORE = new ParseField("score");
        private static final ObjectParser<Builder, Void> PARSER = new ObjectParser<>(
            "google_vertex_ai_rerank_response",
            true,
            Builder::new
        );

        static {
            PARSER.declareString(Builder::setContent, CONTENT);
            PARSER.declareFloat(Builder::setScore, SCORE);
        }

        public static RankedDoc parse(XContentParser parser) {
            Builder builder = PARSER.apply(parser, null);
            return builder.build();
        }

        private static final class Builder {

            private String content;
            private Float score;

            private Builder() {}

            public Builder setScore(Float score) {
                this.score = score;
                return this;
            }

            public Builder setContent(String content) {
                this.content = content;
                return this;
            }

            public RankedDoc build() {
                return new RankedDoc(score, content);
            }
        }
    }
}
