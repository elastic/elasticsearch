/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.xpack.XPackInfoRequest;
import org.elasticsearch.client.xpack.XPackUsageRequest;

import java.util.EnumSet;
import java.util.Locale;
import java.util.stream.Collectors;

final class XPackRequestConverters {

    private XPackRequestConverters() {}

    static Request info(XPackInfoRequest infoRequest) {
        Request request = new Request(HttpGet.METHOD_NAME, "/_xpack");
        if (false == infoRequest.isVerbose()) {
            request.addParameter("human", "false");
        }
        if (false == infoRequest.getCategories().equals(EnumSet.allOf(XPackInfoRequest.Category.class))) {
            request.addParameter("categories", infoRequest.getCategories().stream()
                    .map(c -> c.toString().toLowerCase(Locale.ROOT))
                    .collect(Collectors.joining(",")));
        }
        return request;
    }

    static Request usage(XPackUsageRequest usageRequest) {
        Request request = new Request(HttpGet.METHOD_NAME, "/_xpack/usage");
        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withMasterTimeout(usageRequest.masterNodeTimeout());
        request.addParameters(parameters.asMap());
        return request;
    }
}
