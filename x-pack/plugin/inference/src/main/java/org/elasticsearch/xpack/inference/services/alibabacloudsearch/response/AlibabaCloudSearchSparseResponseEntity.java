/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.response;

import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

public class AlibabaCloudSearchSparseResponseEntity extends AlibabaCloudSearchResponseEntity {
    private static final String FAILED_TO_FIND_FIELD_TEMPLATE =
        "Failed to find required field [%s] in AlibabaCloud Search sparse embeddings response";

    /**
     * Parses the AlibabaCloud Search sparse embedding json response.
     * For a request like:
     *
     * <pre>
     * <code>
     * {
     *  "texts": ["hello this is my name", "I wish I was there!"]
     * }
     * </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     * <code>
     *     {
     *   "request_id": "DDC4306F-xxxx-xxxx-xxxx-92C5CEA756A0",
     *   "latency": 25,
     *   "usage": {
     *     "token_count": 11
     *   },
     *   "result": {
     *     "sparse_embeddings": [
     *       {
     *         "index": 0,
     *         "embedding": [
     *           {
     *             "token_id": 6,
     *             "weight": 0.1014404296875,
     *             "token": ""
     *           },
     *           {
     *             "token_id": 163040,
     *             "weight": 0.2841796875,
     *             "token": "科学技术"
     *           },
     *           {
     *             "token_id": 354,
     *             "weight": 0.1431884765625,
     *             "token": "是"
     *           },
     *           {
     *             "token_id": 5998,
     *             "weight": 0.1614990234375,
     *             "token": "第一"
     *           },
     *           {
     *             "token_id": 8550,
     *             "weight": 0.239013671875,
     *             "token": "生产"
     *           },
     *           {
     *             "token_id": 2017,
     *             "weight": 0.161376953125,
     *             "token": "力"
     *           }
     *         ]
     *       },
     *       {
     *         "index": 1,
     *         "embedding": [
     *           {
     *             "token_id": 9803,
     *             "weight": 0.1949462890625,
     *             "token": "open"
     *           },
     *           {
     *             "token_id": 86250,
     *             "weight": 0.317138671875,
     *             "token": "search"
     *           },
     *           {
     *             "token_id": 5889,
     *             "weight": 0.175048828125,
     *             "token": "产品"
     *           },
     *           {
     *             "token_id": 2564,
     *             "weight": 0.1163330078125,
     *             "token": "文"
     *           },
     *           {
     *             "token_id": 59529,
     *             "weight": 0.16650390625,
     *             "token": "档"
     *           }
     *         ]
     *       }
     *     ]
     *   }
     * }
     * </code>
     * </pre>
     */
    public static SparseEmbeddingResults fromResponse(Request request, HttpResult response) throws IOException {
        return fromResponse(request, response, jsonParser -> {
            positionParserAtTokenAfterField(jsonParser, "sparse_embeddings", FAILED_TO_FIND_FIELD_TEMPLATE);

            List<SparseEmbeddingResults.Embedding> embeddingList = XContentParserUtils.parseList(
                jsonParser,
                AlibabaCloudSearchSparseResponseEntity::parseEmbeddingObject
            );

            return new SparseEmbeddingResults(embeddingList);
        });
    }

    private static SparseEmbeddingResults.Embedding parseEmbeddingObject(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);

        positionParserAtTokenAfterField(parser, "embedding", FAILED_TO_FIND_FIELD_TEMPLATE);

        List<WeightedToken> tokens = parseWeightedTokenList(parser);

        // the parser is currently sitting at an ARRAY_END so go to the next token
        parser.nextToken();
        // if there are additional fields within this object, lets skip them, so we can begin parsing the next embedding array
        parser.skipChildren();

        return new SparseEmbeddingResults.Embedding(tokens, false);
    }

    private static List<WeightedToken> parseWeightedTokenList(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        if (parser.nextToken() == XContentParser.Token.END_ARRAY) {
            return List.of();
        }
        final ArrayList<WeightedToken> list = new ArrayList<>();
        do {
            WeightedToken token = parseEmbeddingList(parser);
            if (token != null) {
                list.add(token);
            }
        } while (parser.nextToken() != XContentParser.Token.END_ARRAY);
        return list;
    }

    private static WeightedToken parseEmbeddingList(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
        Map<String, Object> values = parser.map();
        Object tokenName;
        if (values.containsKey("token")) {
            tokenName = values.get("token");
        } else {
            tokenName = values.get("token_id");
        }
        float weight = Float.parseFloat(values.get("weight").toString());

        if (invalidToken(tokenName) || weight <= 0.0f) {
            return null;
        }

        return new WeightedToken(tokenName.toString(), weight);
    }

    private static boolean invalidToken(Object tokenName) {
        if (tokenName == null) {
            return true;
        }

        String token = tokenName.toString();
        if (token.isEmpty() || token.contains(".")) {
            return true;
        }

        return false;
    }

    private AlibabaCloudSearchSparseResponseEntity() {}
}
