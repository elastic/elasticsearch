/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.audit;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.transport.TransportMessage;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class AuditUtil {

    public static String restRequestContent(RestRequest request) {
        if (request.hasContent()) {
            try {
                return XContentHelper.convertToJson(request.content(), false, false, request.getXContentType());
            } catch (IOException ioe) {
                return "Invalid Format: " + request.content().utf8ToString();
            }
        }
        return "";
    }

    public static Set<String> indices(TransportMessage message) {
        if (message instanceof IndicesRequest) {
            return arrayToSetOrNull(((IndicesRequest) message).indices());
        }
        return null;
    }

    private static Set<String> arrayToSetOrNull(String[] indices) {
        return indices == null ? null : new HashSet<>(Arrays.asList(indices));
    }
}
