/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.response;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

public class AlibabaCloudSearchErrorResponseEntity extends ErrorResponse {
    private static final Logger logger = LogManager.getLogger(AlibabaCloudSearchErrorResponseEntity.class);

    private AlibabaCloudSearchErrorResponseEntity(String errorMessage) {
        super(errorMessage);
    }

    /**
     * An example error response for invalid auth would look like
     * <code>
     *     {
     *      "request_id": "651B3087-8A07-xxxx-xxxx-9C4E7B60F52D",
     *      "latency": 0,
     *      "code": "InvalidParameter",
     *      "message": "JSON parse error: Cannot deserialize value of type `InputType` from String \"xxx\""
     *      }
     * </code>
     *
     *
     * @param response The error response
     * @return An error entity if the response is JSON with the above structure
     * or {@link ErrorResponse#UNDEFINED_ERROR} if the message field wasn't found
     */
    public static ErrorResponse fromResponse(HttpResult response) {
        try (
            XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, response.body())
        ) {
            var responseMap = jsonParser.map();
            if (logger.isDebugEnabled()) {
                logger.debug("Received error response: {}", responseMap);
            }

            var message = (String) responseMap.get("message");
            if (message != null) {
                return new AlibabaCloudSearchErrorResponseEntity(message);
            }
        } catch (Exception e) {
            // swallow the error
        }

        return ErrorResponse.UNDEFINED_ERROR;
    }
}
