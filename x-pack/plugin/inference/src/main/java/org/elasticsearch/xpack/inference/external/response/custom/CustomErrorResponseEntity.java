/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.response.custom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.http.retry.ErrorResponse;

import java.util.Locale;

public class CustomErrorResponseEntity extends ErrorResponse {
    private static final Logger logger = LogManager.getLogger(CustomErrorResponseEntity.class);

    private CustomErrorResponseEntity(String errorMessage) {
        super(errorMessage);
    }

    public static ErrorResponse fromResponse(HttpResult response) {
        try (
            XContentParser jsonParser = XContentFactory.xContent(XContentType.JSON)
                .createParser(XContentParserConfiguration.EMPTY, response.body())
        ) {
            var responseMap = jsonParser.map();
            if (logger.isDebugEnabled()) {
                logger.debug("Received a server error response: {}, body:{}", response.response(), responseMap.toString());
            }

            return new ErrorResponse(
                String.format(Locale.ROOT, "Received a server error response: %s, body: %s", response.response(), responseMap.toString())
            );
        } catch (Exception e) {
            var message = "Parsing custom response body failed. Response: " + response.response();
            logger.warn(message, e);
            return new ErrorResponse(message);
        }
    }
}
