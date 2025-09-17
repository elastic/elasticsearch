/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.response;

import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xpack.inference.external.response.XContentUtils.positionParserAtTokenAfterField;

public class AlibabaCloudSearchCompletionResponseEntity extends AlibabaCloudSearchResponseEntity {
    private static final String FAILED_TO_FIND_FIELD_TEMPLATE =
        "Failed to find required field [%s] in AlibabaCloud Search completion response";

    /**
     * Parses the AlibabaCloud Search embedding json response.
     * For a request like:
     *
     * <pre>
     * <code>
     * {
     *     "messages": [
     *         {
     *             "role": "system",
     *             "content": "你是一个机器人助手"
     *         },
     *         {
     *             "role": "user",
     *             "content": "河南的省会是哪里"
     *         },
     *         {
     *             "role": "assistant",
     *             "content": "郑州"
     *         },
     *         {
     *             "role": "user",
     *             "content": "那里有什么好玩的"
     *         }
     *     ],
     *     "stream": false
     * }
     * </code>
     * </pre>
     *
     * The response would look like:
     *
     * <pre>
     * <code>
     * {
     *   "request_id": "450fcb80-f796-46c1-8d69-e1e86d29aa9f",
     *   "latency": 564.903929,
     *   "result": {
     *     "text":"郑州是一个历史文化悠久且现代化的城市，有很多好玩的地方。以下是一些推荐的旅游景点：
     *     嵩山少林寺：作为少林武术的发源地，嵩山少林寺一直以来都是游客向往的地方。在这里，你可以欣赏到精彩的武术表演，领略少林功夫的魅力。
     *     黄河游览区：黄河是中华民族的母亲河，而在郑州，你可以乘坐游船观赏黄河的多种风情，感受大河之美。
     *     郑州动物园：这是一个适合全家游玩的景点，拥有各种珍稀动物，如大熊猫、金丝猴等，让孩子们近距离接触动物，增长见识。
     *     郑州博物馆：如果你对历史文化感兴趣，那么郑州博物馆是一个不错的选择。这里收藏了大量珍贵的文物，展示了郑州地区的历史变迁和文化传承。
     *     郑州世纪公园：这是一个大型的城市公园，拥有美丽的湖泊、花园和休闲设施。在这里，你可以进行散步、慢跑等户外活动，享受大自然的宁静与和谐。
     *     以上只是郑州众多好玩地方的一部分，实际上郑州还有很多其他值得一游的景点。希望你在郑州的旅行能够愉快！"
     *   }
     *   "usage": {
     *       "output_tokens": 6320,
     *       "input_tokens": 35,
     *       "total_tokens": 6355,
     *   }
     *
     * }
     * </code>
     * </pre>
     */

    public static ChatCompletionResults fromResponse(Request request, HttpResult response) throws IOException {
        return fromResponse(request, response, jsonParser -> {
            positionParserAtTokenAfterField(jsonParser, "text", FAILED_TO_FIND_FIELD_TEMPLATE);

            XContentParser.Token contentToken = jsonParser.currentToken();
            ensureExpectedToken(XContentParser.Token.VALUE_STRING, contentToken, jsonParser);
            String content = jsonParser.text();

            return new ChatCompletionResults(List.of(new ChatCompletionResults.Result(content)));
        });
    }
}
