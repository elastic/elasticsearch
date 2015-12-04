/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.audit;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.TransportMessage;

import java.io.IOException;

/**
 *
 */
public class AuditUtil {

    public static String restRequestContent(RestRequest request) {
        if (request.hasContent()) {
            try {
                return XContentHelper.convertToJson(request.content(), false, false);
            } catch (IOException ioe) {
                return "Invalid Format: " + request.content().toUtf8();
            }
        }
        return "";
    }

    public static String[] indices(TransportMessage message) {
        if (message instanceof IndicesRequest) {
            return ((IndicesRequest) message).indices();
        }
        return null;
    }
}
