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

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

public class SearchTemplateResponse  extends ActionResponse implements StatusToXContent {

    /** Contains the source of the rendered template **/
    private BytesReference source;

    /** Contains the search response, if any **/
    private SearchResponse response;

    SearchTemplateResponse() {
    }

    public BytesReference getSource() {
        return source;
    }

    public void setSource(BytesReference source) {
        this.source = source;
    }

    public SearchResponse getResponse() {
        return response;
    }

    public void setResponse(SearchResponse searchResponse) {
        this.response = searchResponse;
    }

    public boolean hasResponse() {
        return response != null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalBytesReference(source);
        out.writeOptionalStreamable(response);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        source = in.readOptionalBytesReference();
        response = in.readOptionalStreamable(SearchResponse::new);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (hasResponse()) {
            response.toXContent(builder, params);
        } else {
            builder.rawField("template_output", source);
        }
        return builder;
    }

    @Override
    public RestStatus status() {
        if (hasResponse()) {
            return response.status();
        } else {
            return RestStatus.OK;
        }
    }
}
