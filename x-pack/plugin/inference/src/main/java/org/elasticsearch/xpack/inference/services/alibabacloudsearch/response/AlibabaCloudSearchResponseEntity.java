/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.response;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.request.AlibabaCloudSearchRequest;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.response.XContentUtils.moveToFirstToken;

public abstract class AlibabaCloudSearchResponseEntity {
    private static final Logger logger = LogManager.getLogger(AlibabaCloudSearchResponseEntity.class);

    public static <R> R fromResponse(Request request, HttpResult response, CheckedFunction<XContentParser, R, IOException> function)
        throws IOException {
        var parserConfig = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        AlibabaCloudSearchRequest alibabaCloudSearchRequest = (AlibabaCloudSearchRequest) request;

        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(parserConfig, response.body())) {
            moveToFirstToken(parser);

            XContentParser.Token token = parser.currentToken();
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);

            R result = null;
            String requestID = null;
            float latency = 0;
            Map<String, Object> usage = null;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String currentFieldName = parser.currentName();
                parser.nextToken();
                switch (currentFieldName) {
                    case "result":
                        result = function.apply(parser);
                        break;
                    case "request_id":
                        requestID = parser.text();
                        break;
                    case "latency":
                        latency = parser.floatValue();
                        break;
                    case "usage":
                        usage = parser.map();
                        break;
                    default:
                        parser.skipChildren();
                }
            }

            logger.debug(
                "AlibabaCloud Search uri [{}] response: request_id [{}], latency [{}ms], client cost [{}ms], usage [{}]",
                request.getURI().getPath(),
                requestID,
                latency,
                System.currentTimeMillis() - alibabaCloudSearchRequest.getStartTime(),
                usage
            );
            return result;
        }
    }
}
