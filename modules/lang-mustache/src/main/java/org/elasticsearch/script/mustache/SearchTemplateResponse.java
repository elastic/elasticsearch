/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class SearchTemplateResponse extends ActionResponse implements StatusToXContentObject {
    public static ParseField TEMPLATE_OUTPUT_FIELD = new ParseField("template_output");

    /** Contains the source of the rendered template **/
    private BytesReference source;

    /** Contains the search response, if any **/
    private SearchResponse response;

    SearchTemplateResponse() {}

    SearchTemplateResponse(StreamInput in) throws IOException {
        super(in);
        source = in.readOptionalBytesReference();
        response = in.readOptionalWriteable(SearchResponse::new);
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
    public String toString() {
        return "SearchTemplateResponse [source=" + source + ", response=" + response + "]";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBytesReference(source);
        out.writeOptionalWriteable(response);
    }

    public static SearchTemplateResponse fromXContent(XContentParser parser) throws IOException {
        SearchTemplateResponse searchTemplateResponse = new SearchTemplateResponse();
        Map<String, Object> contentAsMap = parser.map();

        if (contentAsMap.containsKey(TEMPLATE_OUTPUT_FIELD.getPreferredName())) {
            Object source = contentAsMap.get(TEMPLATE_OUTPUT_FIELD.getPreferredName());
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).value(source);
            searchTemplateResponse.setSource(BytesReference.bytes(builder));
        } else {
            XContentType contentType = parser.contentType();
            XContentBuilder builder = XContentFactory.contentBuilder(contentType).map(contentAsMap);
            XContentParser searchResponseParser = contentType.xContent()
                .createParser(parser.getXContentRegistry(), parser.getDeprecationHandler(), BytesReference.bytes(builder).streamInput());

            SearchResponse searchResponse = SearchResponse.fromXContent(searchResponseParser);
            searchTemplateResponse.setResponse(searchResponse);
        }
        return searchTemplateResponse;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (hasResponse()) {
            response.toXContent(builder, params);
        } else {
            builder.startObject();
            // we can assume the template is always json as we convert it before compiling it
            try (InputStream stream = source.streamInput()) {
                builder.rawField(TEMPLATE_OUTPUT_FIELD.getPreferredName(), stream, XContentType.JSON);
            }
            builder.endObject();
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
