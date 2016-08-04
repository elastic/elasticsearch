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
package org.elasticsearch.rest.action.support;

import org.elasticsearch.common.xcontent.StatusToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;

import java.util.function.Function;

/**
 * Content listener that extracts that {@link RestStatus} from the response.
 */
public class RestStatusToXContentListener<Response extends StatusToXContent> extends RestResponseListener<Response> {
    private final Function<Response, String> extractLocation;

    /**
     * Build an instance that doesn't support responses with the status {@code 201 CREATED}.
     */
    public RestStatusToXContentListener(RestChannel channel) {
        this(channel, r -> {
            assert false: "Returned a 201 CREATED but not set up to support a Location header";
            return null;
        });
    }

    /**
     * Build an instance that does support responses with the status {@code 201 CREATED}.
     */
    public RestStatusToXContentListener(RestChannel channel, Function<Response, String> extractLocation) {
        super(channel);
        this.extractLocation = extractLocation;
    }

    @Override
    public final RestResponse buildResponse(Response response) throws Exception {
        return buildResponse(response, channel.newBuilder());
    }

    public final RestResponse buildResponse(Response response, XContentBuilder builder) throws Exception {
        builder.startObject();
        response.toXContent(builder, channel.request());
        builder.endObject();
        BytesRestResponse restResponse = new BytesRestResponse(response.status(), builder);
        if (RestStatus.CREATED == restResponse.status()) {
            String location = extractLocation.apply(response);
            if (location != null) {
                restResponse.addHeader("Location", location);
            }
        }
        return restResponse;
    }
}
