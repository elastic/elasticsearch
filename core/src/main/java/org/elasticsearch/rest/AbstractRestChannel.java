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
package org.elasticsearch.rest;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toSet;

public abstract class AbstractRestChannel implements RestChannel {

    private static final Predicate<String> INCLUDE_FILTER = f -> f.charAt(0) != '-';
    private static final Predicate<String> EXCLUDE_FILTER = INCLUDE_FILTER.negate();

    protected final RestRequest request;
    protected final boolean detailedErrorsEnabled;
    private final String format;
    private final String filterPath;
    private final boolean pretty;
    private final boolean human;

    private BytesStreamOutput bytesOut;

    protected AbstractRestChannel(RestRequest request, boolean detailedErrorsEnabled) {
        this.request = request;
        this.detailedErrorsEnabled = detailedErrorsEnabled;
        this.format = request.param("format", request.header("Accept"));
        this.filterPath = request.param("filter_path", null);
        this.pretty = request.paramAsBoolean("pretty", false);
        this.human = request.paramAsBoolean("human", false);
    }

    @Override
    public XContentBuilder newBuilder() throws IOException {
        return newBuilder(request.getXContentType(), true);
    }

    @Override
    public XContentBuilder newErrorBuilder() throws IOException {
        // Disable filtering when building error responses
        return newBuilder(request.getXContentType(), false);
    }

    @Override
    public XContentBuilder newBuilder(@Nullable XContentType xContentType, boolean useFiltering) throws IOException {
        XContentType contentType = XContentType.fromMediaTypeOrFormat(format);
        if (contentType == null) {
            if (xContentType != null) {
                contentType = xContentType;
            } else {
                // default to JSON output
                contentType = XContentType.JSON;
            }
        }

        Set<String> includes = Collections.emptySet();
        Set<String> excludes = Collections.emptySet();
        if (useFiltering) {
            Set<String> filters = Strings.splitStringByCommaToSet(filterPath);
            includes = filters.stream().filter(INCLUDE_FILTER).collect(toSet());
            excludes = filters.stream().filter(EXCLUDE_FILTER).map(f -> f.substring(1)).collect(toSet());
        }

        XContentBuilder builder = new XContentBuilder(XContentFactory.xContent(contentType), bytesOutput(), includes, excludes);
        if (pretty) {
            builder.prettyPrint().lfAtEnd();
        }

        builder.humanReadable(human);
        return builder;
    }

    /**
     * A channel level bytes output that can be reused. It gets reset on each call to this
     * method.
     */
    @Override
    public final BytesStreamOutput bytesOutput() {
        if (bytesOut == null) {
            bytesOut = newBytesOutput();
        } else {
            bytesOut.reset();
        }
        return bytesOut;
    }

    protected BytesStreamOutput newBytesOutput() {
        return new BytesStreamOutput();
    }

    @Override
    public RestRequest request() {
        return this.request;
    }

    @Override
    public boolean detailedErrorsEnabled() {
        return detailedErrorsEnabled;
    }

    @Override
    public void sendErrorResponse(RestStatus restStatus, String errorMessage) throws IOException {
        sendResponse(new BytesRestResponse(restStatus, newErrorBuilder().startObject()
            .field("error", errorMessage)
            .endObject()));
    }
}
