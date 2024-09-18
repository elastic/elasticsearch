/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.LeakTracker;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.InputStream;

public class SearchTemplateResponse extends ActionResponse implements ToXContentObject {
    public static ParseField TEMPLATE_OUTPUT_FIELD = new ParseField("template_output");

    /** Contains the source of the rendered template **/
    private BytesReference source;

    /** Contains the search response, if any **/
    private SearchResponse response;

    private final RefCounted refCounted = LeakTracker.wrap(new AbstractRefCounted() {
        @Override
        protected void closeInternal() {
            if (response != null) {
                response.decRef();
            }
        }
    });

    SearchTemplateResponse() {}

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

    @Override
    public void incRef() {
        refCounted.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return refCounted.tryIncRef();
    }

    @Override
    public boolean decRef() {
        return refCounted.decRef();
    }

    @Override
    public boolean hasReferences() {
        return refCounted.hasReferences();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    void innerToXContent(XContentBuilder builder, Params params) throws IOException {
        if (hasResponse()) {
            ChunkedToXContent.wrapAsToXContent(response::innerToXContentChunked).toXContent(builder, params);
        } else {
            // we can assume the template is always json as we convert it before compiling it
            try (InputStream stream = source.streamInput()) {
                builder.rawField(TEMPLATE_OUTPUT_FIELD.getPreferredName(), stream, XContentType.JSON);
            }
        }
    }

    public RestStatus status() {
        if (hasResponse()) {
            return response.status();
        } else {
            return RestStatus.OK;
        }
    }
}
