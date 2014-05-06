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

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.elasticsearch.search.Scroll.readScroll;

/**
 * A request to execute search against one or more indices (or all). Best created using
 * {@link org.elasticsearch.client.Requests#searchRequest(String...)}.
 * <p/>
 * <p>Note, the search {@link #source(org.elasticsearch.search.builder.SearchSourceBuilder)}
 * is required. The search source is the different search options, including facets and such.
 * <p/>
 * <p>There is an option to specify an addition search source using the {@link #extraSource(org.elasticsearch.search.builder.SearchSourceBuilder)}.
 *
 * @see org.elasticsearch.client.Requests#searchRequest(String...)
 * @see org.elasticsearch.client.Client#search(SearchRequest)
 * @see SearchResponse
 */
public class SearchRequest extends ActionRequest<SearchRequest> {

    private SearchType searchType = SearchType.DEFAULT;

    private String[] indices;

    @Nullable
    private String routing;
    @Nullable
    private String preference;

    private BytesReference templateSource;
    private boolean templateSourceUnsafe;
    private String templateName;
    private Map<String, String> templateParams = Collections.emptyMap();

    private BytesReference source;
    private boolean sourceUnsafe;

    private BytesReference extraSource;
    private boolean extraSourceUnsafe;

    private Scroll scroll;

    private String[] types = Strings.EMPTY_ARRAY;

    private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

    public SearchRequest() {
    }

    /**
     * Constructs a new search request against the indices. No indices provided here means that search
     * will run against all indices.
     */
    public SearchRequest(String... indices) {
        indices(indices);
    }

    /**
     * Constructs a new search request against the provided indices with the given search source.
     */
    public SearchRequest(String[] indices, byte[] source) {
        indices(indices);
        this.source = new BytesArray(source);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        // no need to check, we resolve to match all query
//        if (source == null && extraSource == null) {
//            validationException = addValidationError("search source is missing", validationException);
//        }
        return validationException;
    }

    public void beforeStart() {
        // we always copy over if needed, the reason is that a request might fail while being search remotely
        // and then we need to keep the buffer around
        if (source != null && sourceUnsafe) {
            source = source.copyBytesArray();
            sourceUnsafe = false;
        }
        if (extraSource != null && extraSourceUnsafe) {
            extraSource = extraSource.copyBytesArray();
            extraSourceUnsafe = false;
        }
        if (templateSource != null && templateSourceUnsafe) {
            templateSource = templateSource.copyBytesArray();
            templateSourceUnsafe = false;
        }
    }

    /**
     * Sets the indices the search will be executed on.
     */
    public SearchRequest indices(String... indices) {
        if (indices == null) {
            throw new ElasticsearchIllegalArgumentException("indices must not be null");
        } else {
            for (int i = 0; i < indices.length; i++) {
                if (indices[i] == null) {
                    throw new ElasticsearchIllegalArgumentException("indices[" + i +"] must not be null");
                }
            }
        }
        this.indices = indices;
        return this;
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public SearchRequest indicesOptions(IndicesOptions indicesOptions) {
        this.indicesOptions = indicesOptions;
        return this;
    }

    /**
     * The document types to execute the search against. Defaults to be executed against
     * all types.
     */
    public String[] types() {
        return types;
    }

    /**
     * The document types to execute the search against. Defaults to be executed against
     * all types.
     */
    public SearchRequest types(String... types) {
        this.types = types;
        return this;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public String routing() {
        return this.routing;
    }

    /**
     * A comma separated list of routing values to control the shards the search will be executed on.
     */
    public SearchRequest routing(String routing) {
        this.routing = routing;
        return this;
    }

    /**
     * The routing values to control the shards that the search will be executed on.
     */
    public SearchRequest routing(String... routings) {
        this.routing = Strings.arrayToCommaDelimitedString(routings);
        return this;
    }

    /**
     * Sets the preference to execute the search. Defaults to randomize across shards. Can be set to
     * <tt>_local</tt> to prefer local shards, <tt>_primary</tt> to execute only on primary shards, or
     * a custom value, which guarantees that the same order will be used across different requests.
     */
    public SearchRequest preference(String preference) {
        this.preference = preference;
        return this;
    }

    public String preference() {
        return this.preference;
    }

    /**
     * The search type to execute, defaults to {@link SearchType#DEFAULT}.
     */
    public SearchRequest searchType(SearchType searchType) {
        this.searchType = searchType;
        return this;
    }

    /**
     * The a string representation search type to execute, defaults to {@link SearchType#DEFAULT}. Can be
     * one of "dfs_query_then_fetch"/"dfsQueryThenFetch", "dfs_query_and_fetch"/"dfsQueryAndFetch",
     * "query_then_fetch"/"queryThenFetch", and "query_and_fetch"/"queryAndFetch".
     */
    public SearchRequest searchType(String searchType) throws ElasticsearchIllegalArgumentException {
        return searchType(SearchType.fromString(searchType));
    }

    /**
     * The source of the search request.
     */
    public SearchRequest source(SearchSourceBuilder sourceBuilder) {
        this.source = sourceBuilder.buildAsBytes(Requests.CONTENT_TYPE);
        this.sourceUnsafe = false;
        return this;
    }

    /**
     * The source of the search request. Consider using either {@link #source(byte[])} or
     * {@link #source(org.elasticsearch.search.builder.SearchSourceBuilder)}.
     */
    public SearchRequest source(String source) {
        this.source = new BytesArray(source);
        this.sourceUnsafe = false;
        return this;
    }

    /**
     * The source of the search request in the form of a map.
     */
    public SearchRequest source(Map source) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
            builder.map(source);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    public SearchRequest source(XContentBuilder builder) {
        this.source = builder.bytes();
        this.sourceUnsafe = false;
        return this;
    }

    /**
     * The search source to execute.
     */
    public SearchRequest source(byte[] source) {
        return source(source, 0, source.length, false);
    }


    /**
     * The search source to execute.
     */
    public SearchRequest source(byte[] source, int offset, int length) {
        return source(source, offset, length, false);
    }

    /**
     * The search source to execute.
     */
    public SearchRequest source(byte[] source, int offset, int length, boolean unsafe) {
        return source(new BytesArray(source, offset, length), unsafe);
    }

    /**
     * The search source to execute.
     */
    public SearchRequest source(BytesReference source, boolean unsafe) {
        this.source = source;
        this.sourceUnsafe = unsafe;
        return this;
    }

    /**
     * The search source to execute.
     */
    public BytesReference source() {
        return source;
    }

    /**
     * The search source template to execute.
     */
    public BytesReference templateSource() {
        return templateSource;
    }

    /**
     * Allows to provide additional source that will be used as well.
     */
    public SearchRequest extraSource(SearchSourceBuilder sourceBuilder) {
        if (sourceBuilder == null) {
            extraSource = null;
            return this;
        }
        this.extraSource = sourceBuilder.buildAsBytes(Requests.CONTENT_TYPE);
        this.extraSourceUnsafe = false;
        return this;
    }

    public SearchRequest extraSource(Map extraSource) {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(Requests.CONTENT_TYPE);
            builder.map(extraSource);
            return extraSource(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    public SearchRequest extraSource(XContentBuilder builder) {
        this.extraSource = builder.bytes();
        this.extraSourceUnsafe = false;
        return this;
    }

    /**
     * Allows to provide additional source that will use used as well.
     */
    public SearchRequest extraSource(String source) {
        this.extraSource = new BytesArray(source);
        this.extraSourceUnsafe = false;
        return this;
    }

    /**
     * Allows to provide additional source that will be used as well.
     */
    public SearchRequest extraSource(byte[] source) {
        return extraSource(source, 0, source.length, false);
    }

    /**
     * Allows to provide additional source that will be used as well.
     */
    public SearchRequest extraSource(byte[] source, int offset, int length) {
        return extraSource(source, offset, length, false);
    }

    /**
     * Allows to provide additional source that will be used as well.
     */
    public SearchRequest extraSource(byte[] source, int offset, int length, boolean unsafe) {
        return extraSource(new BytesArray(source, offset, length), unsafe);
    }

    /**
     * Allows to provide additional source that will be used as well.
     */
    public SearchRequest extraSource(BytesReference source, boolean unsafe) {
        this.extraSource = source;
        this.extraSourceUnsafe = unsafe;
        return this;
    }

    /**
     * Allows to provide template as source.
     */
    public SearchRequest templateSource(BytesReference template, boolean unsafe) {
        this.templateSource = template;
        this.templateSourceUnsafe = unsafe;
        return this;
    }

    /**
     * The template of the search request.
     */
    public SearchRequest templateSource(String template) {
        this.templateSource = new BytesArray(template);
        this.templateSourceUnsafe = false;
        return this;
    }

    /**
     * The name of the stored template
     */
    public void templateName(String name) {
        this.templateName = name;
    }

    /**
     * Template parameters used for rendering
     */
    public void templateParams(Map<String, String> params) {
        this.templateParams = params;
    }

    /**
     * The name of the stored template
     */
    public String templateName() {
        return templateName;
    }

    /**
     * Template parameters used for rendering
     */
    public Map<String, String> templateParams() {
        return templateParams;
    }

    /**
     * Additional search source to execute.
     */
    public BytesReference extraSource() {
        return this.extraSource;
    }

    /**
     * The tye of search to execute.
     */
    public SearchType searchType() {
        return searchType;
    }

    /**
     * The indices
     */
    public String[] indices() {
        return indices;
    }

    /**
     * If set, will enable scrolling of the search request.
     */
    public Scroll scroll() {
        return scroll;
    }

    /**
     * If set, will enable scrolling of the search request.
     */
    public SearchRequest scroll(Scroll scroll) {
        this.scroll = scroll;
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public SearchRequest scroll(TimeValue keepAlive) {
        return scroll(new Scroll(keepAlive));
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public SearchRequest scroll(String keepAlive) {
        return scroll(new Scroll(TimeValue.parseTimeValue(keepAlive, null)));
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.getVersion().before(Version.V_1_2_0)) {
            in.readByte(); // backward comp. for operation threading
        }
        searchType = SearchType.fromId(in.readByte());

        indices = new String[in.readVInt()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = in.readString();
        }

        routing = in.readOptionalString();
        preference = in.readOptionalString();

        if (in.readBoolean()) {
            scroll = readScroll(in);
        }

        sourceUnsafe = false;
        source = in.readBytesReference();

        extraSourceUnsafe = false;
        extraSource = in.readBytesReference();

        types = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);

        if (in.getVersion().onOrAfter(Version.V_1_1_0)) {
            templateSourceUnsafe = false;
            templateSource = in.readBytesReference();
            templateName =  in.readOptionalString();
            if (in.readBoolean()) {
                templateParams = (Map<String, String>) in.readGenericValue();
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getVersion().before(Version.V_1_2_0)) {
            out.writeByte((byte) 2); // operation threading
        }
        out.writeByte(searchType.id());

        out.writeVInt(indices.length);
        for (String index : indices) {
            out.writeString(index);
        }

        out.writeOptionalString(routing);
        out.writeOptionalString(preference);

        if (scroll == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            scroll.writeTo(out);
        }
        out.writeBytesReference(source);
        out.writeBytesReference(extraSource);
        out.writeStringArray(types);
        indicesOptions.writeIndicesOptions(out);

        if (out.getVersion().onOrAfter(Version.V_1_1_0)) {
            out.writeBytesReference(templateSource);
            out.writeOptionalString(templateName);

            boolean existTemplateParams = templateParams != null;
            out.writeBoolean(existTemplateParams);
            if (existTemplateParams) {
                out.writeGenericValue(templateParams);
            }
        }
    }
}
