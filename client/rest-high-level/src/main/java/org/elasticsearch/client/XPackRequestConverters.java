/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
