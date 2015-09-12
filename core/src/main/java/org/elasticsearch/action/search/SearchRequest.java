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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.Template;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.search.Scroll.readScroll;

/**
 * A request to execute search against one or more indices (or all). Best created using
 * {@link org.elasticsearch.client.Requests#searchRequest(String...)}.
 * <p/>
 * <p>Note, the search {@link #source(org.elasticsearch.search.builder.SearchSourceBuilder)}
 * is required. The search source is the different search options, including aggregations and such.
 * <p/>
 * <p>There is an option to specify an addition search source using the {@link #extraSource(org.elasticsearch.search.builder.SearchSourceBuilder)}.
 *
 * @see org.elasticsearch.client.Requests#searchRequest(String...)
 * @see org.elasticsearch.client.Client#search(SearchRequest)
 * @see SearchResponse
 */
public class SearchRequest extends ActionRequest<SearchRequest> implements IndicesRequest.Replaceable {

    private SearchType searchType = SearchType.DEFAULT;

    private String[] indices;

    @Nullable
    private String routing;
    @Nullable
    private String preference;

    private BytesReference templateSource;
    private Template template;

    private BytesReference source;

    private BytesReference extraSource;
    private Boolean requestCache;

    private Scroll scroll;

    private String[] types = Strings.EMPTY_ARRAY;

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.strictExpandOpenAndForbidClosed();

    private IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;

    public SearchRequest() {
    }

    /**
     * Copy constructor that creates a new search request that is a copy of the one provided as an argument.
     * The new request will inherit though headers and context from the original request that caused it.
     */
    public SearchRequest(SearchRequest searchRequest, ActionRequest originalRequest) {
        super(originalRequest);
        this.searchType = searchRequest.searchType;
        this.indices = searchRequest.indices;
        this.routing = searchRequest.routing;
        this.preference = searchRequest.preference;
        this.templateSource = searchRequest.templateSource;
        this.template = searchRequest.template;
        this.source = searchRequest.source;
        this.extraSource = searchRequest.extraSource;
        this.requestCache = searchRequest.requestCache;
        this.scroll = searchRequest.scroll;
        this.types = searchRequest.types;
        this.indicesOptions = searchRequest.indicesOptions;
    }

    /**
     * Constructs a new search request starting from the provided request, meaning that it will
     * inherit its headers and context
     */
    public SearchRequest(ActionRequest request) {
        super(request);
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

    /**
     * Sets the indices the search will be executed on.
     */
    @Override
    public SearchRequest indices(String... indices) {
        if (indices == null) {
            throw new IllegalArgumentException("indices must not be null");
        } else {
            for (int i = 0; i < indices.length; i++) {
                if (indices[i] == null) {
                    throw new IllegalArgumentException("indices[" + i + "] must not be null");
                }
            }
        }
        this.indices = indices;
        return this;
    }

    @Override
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
    public SearchRequest searchType(String searchType) {
        return searchType(SearchType.fromString(searchType, ParseFieldMatcher.EMPTY));
    }

    /**
     * The source of the search request.
     */
    public SearchRequest source(SearchSourceBuilder sourceBuilder) {
        this.source = sourceBuilder.buildAsBytes(Requests.CONTENT_TYPE);
        return this;
    }

    /**
     * The search source to execute.
     */
    public SearchRequest source(BytesReference source) {
        this.source = source;
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
        return this;
    }

    /**
     * Allows to provide template as source.
     */
    public SearchRequest templateSource(BytesReference template) {
        this.templateSource = template;
        return this;
    }

    /**
     * The template of the search request.
     */
    public SearchRequest templateSource(String template) {
        this.templateSource = new BytesArray(template);
        return this;
    }

    /**
     * The stored template
     */
    public void template(Template template) {
        this.template = template;
    }

    /**
     * The stored template
     */
    public Template template() {
        return template;
    }

    /**
     * The name of the stored template
     * 
     * @deprecated use {@link #template(Template))} instead.
     */
    @Deprecated
    public void templateName(String templateName) {
        updateOrCreateScript(templateName, null, null, null);
    }

    /**
     * The type of the stored template
     * 
     * @deprecated use {@link #template(Template))} instead.
     */
    @Deprecated
    public void templateType(ScriptService.ScriptType templateType) {
        updateOrCreateScript(null, templateType, null, null);
    }

    /**
     * Template parameters used for rendering
     * 
     * @deprecated use {@link #template(Template))} instead.
     */
    @Deprecated
    public void templateParams(Map<String, Object> params) {
        updateOrCreateScript(null, null, null, params);
    }

    /**
     * The name of the stored template
     * 
     * @deprecated use {@link #template()} instead.
     */
    @Deprecated
    public String templateName() {
        return template == null ? null : template.getScript();
    }

    /**
     * The name of the stored template
     * 
     * @deprecated use {@link #template()} instead.
     */
    @Deprecated
    public ScriptService.ScriptType templateType() {
        return template == null ? null : template.getType();
    }

    /**
     * Template parameters used for rendering
     * 
     * @deprecated use {@link #template()} instead.
     */
    @Deprecated
    public Map<String, Object> templateParams() {
        return template == null ? null : template.getParams();
    }

    private void updateOrCreateScript(String templateContent, ScriptType type, String lang, Map<String, Object> params) {
        Template template = template();
        if (template == null) {
            template = new Template(templateContent == null ? "" : templateContent, type == null ? ScriptType.INLINE : type, lang, null,
                    params);
        } else {
            String newTemplateContent = templateContent == null ? template.getScript() : templateContent;
            ScriptType newTemplateType = type == null ? template.getType() : type;
            String newTemplateLang = lang == null ? template.getLang() : lang;
            Map<String, Object> newTemplateParams = params == null ? template.getParams() : params;
            template = new Template(newTemplateContent, newTemplateType, MustacheScriptEngineService.NAME, null, newTemplateParams);
        }
        template(template);
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
    @Override
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
        return scroll(new Scroll(TimeValue.parseTimeValue(keepAlive, null, getClass().getSimpleName() + ".Scroll.keepAlive")));
    }

    /**
     * Sets if this request should use the request cache or not, assuming that it can (for
     * example, if "now" is used, it will never be cached). By default (not set, or null,
     * will default to the index level setting if request cache is enabled or not).
     */
    public SearchRequest requestCache(Boolean requestCache) {
        this.requestCache = requestCache;
        return this;
    }

    public Boolean requestCache() {
        return this.requestCache;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
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

        source = in.readBytesReference();
        extraSource = in.readBytesReference();

        types = in.readStringArray();
        indicesOptions = IndicesOptions.readIndicesOptions(in);

        templateSource = in.readBytesReference();
        if (in.readBoolean()) {
            template = Template.readTemplate(in);
        }
        requestCache = in.readOptionalBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
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

        out.writeBytesReference(templateSource);
        boolean hasTemplate = template != null;
        out.writeBoolean(hasTemplate);
        if (hasTemplate) {
            template.writeTo(out);
        }

        out.writeOptionalBoolean(requestCache);
    }
}
