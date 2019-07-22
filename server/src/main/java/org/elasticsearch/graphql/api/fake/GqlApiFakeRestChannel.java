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

package org.elasticsearch.graphql.api.fake;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.graphql.api.GqlApiUtils;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class GqlApiFakeRestChannel implements RestChannel {
    BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
    XContentBuilder builder;
    RestRequest innerRequest;
    CompletableFuture promise;

    public GqlApiFakeRestChannel(XContentBuilder builder, RestRequest innerRequest, CompletableFuture promise) {
        this.builder = builder;
        this.innerRequest = innerRequest;
        this.promise = promise;
    }

    @Override
    public XContentBuilder newBuilder() throws IOException {
        return builder;
    }

    @Override
    public XContentBuilder newErrorBuilder() throws IOException {
        return builder;
    }

    @Override
    public XContentBuilder newBuilder(XContentType xContentType, boolean useFiltering) throws IOException {
        return builder;
    }

    @Override
    public XContentBuilder newBuilder(XContentType xContentType, XContentType responseContentType, boolean useFiltering) throws IOException {
        return builder;
    }

    @Override
    public BytesStreamOutput bytesOutput() {
        return bytesStreamOutput;
    }

    @Override
    public RestRequest request() {
        return innerRequest;
    }

    @Override
    public boolean detailedErrorsEnabled() {
        return false;
    }

    @Override
    public void sendResponse(RestResponse response) {
        try {
            List<Object> result = (List) GqlApiUtils.getJavaUtilBuilderResult(builder);
            promise.complete(result);
        } catch (Exception e) {
            promise.completeExceptionally(e);
        }
    }
}
