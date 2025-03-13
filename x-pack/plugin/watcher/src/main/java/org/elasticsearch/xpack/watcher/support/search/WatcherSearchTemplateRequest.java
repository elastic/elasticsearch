/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.support.search;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * A {@link WatcherSearchTemplateRequest} contains the search request and the eventual template that will
 * be rendered as a script by {@link WatcherSearchTemplateService} before being executed.
 */
public class WatcherSearchTemplateRequest implements ToXContentObject {

    private final String[] indices;
    private final SearchType searchType;
    private final IndicesOptions indicesOptions;
    private final Script template;
    private final BytesReference searchSource;
    private boolean restTotalHitsAsInt = true;

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(WatcherSearchTemplateRequest.class);
    static final String TYPES_DEPRECATION_MESSAGE =
        "[types removal] Specifying empty types array in a watcher search request is deprecated.";

    public WatcherSearchTemplateRequest(
        String[] indices,
        SearchType searchType,
        IndicesOptions indicesOptions,
        BytesReference searchSource
    ) {
        this.indices = indices;
        this.searchType = searchType;
        this.indicesOptions = indicesOptions;
        // Here we convert a watch search request body into an inline search template,
        // this way if any Watcher related context variables are used, they will get resolved.
        this.template = new Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, searchSource.utf8ToString(), Collections.emptyMap());
        this.searchSource = BytesArray.EMPTY;
    }

    public WatcherSearchTemplateRequest(String[] indices, SearchType searchType, IndicesOptions indicesOptions, Script template) {
        this.indices = indices;
        this.searchType = searchType;
        this.indicesOptions = indicesOptions;
        this.template = template;
        this.searchSource = BytesArray.EMPTY;
    }

    public WatcherSearchTemplateRequest(WatcherSearchTemplateRequest original, BytesReference source) {
        this.indices = original.indices;
        this.searchType = original.searchType;
        this.indicesOptions = original.indicesOptions;
        this.searchSource = source;
        this.template = original.template;
        this.restTotalHitsAsInt = original.restTotalHitsAsInt;
    }

    private WatcherSearchTemplateRequest(
        String[] indices,
        SearchType searchType,
        IndicesOptions indicesOptions,
        BytesReference searchSource,
        Script template
    ) {
        this.indices = indices;
        this.searchType = searchType;
        this.indicesOptions = indicesOptions;
        this.template = template;
        this.searchSource = searchSource;
    }

    @Nullable
    public Script getTemplate() {
        return template;
    }

    public String[] getIndices() {
        return indices;
    }

    public SearchType getSearchType() {
        return searchType;
    }

    public IndicesOptions getIndicesOptions() {
        return indicesOptions;
    }

    public boolean isRestTotalHitsAsint() {
        return restTotalHitsAsInt;
    }

    /**
     * Indicates whether the total hits in the response should be
     * serialized as number (<code>true</code>) or as an object (<code>false</code>).
     * Defaults to false.
     */
    public void setRestTotalHitsAsInt(boolean value) {
        this.restTotalHitsAsInt = value;
    }

    public BytesReference getSearchSource() {
        return searchSource;
    }

    public Script getOrCreateTemplate() {
        if (template != null) {
            return template;
        } else {
            return new Script(ScriptType.INLINE, Script.DEFAULT_TEMPLATE_LANG, searchSource.utf8ToString(), Collections.emptyMap());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (searchType != null) {
            builder.field(SEARCH_TYPE_FIELD.getPreferredName(), searchType.toString().toLowerCase(Locale.ENGLISH));
        }
        if (indices != null) {
            builder.array(INDICES_FIELD.getPreferredName(), indices);
        }

        builder.field(REST_TOTAL_HITS_AS_INT_FIELD.getPreferredName(), restTotalHitsAsInt);

        if (searchSource != null && searchSource.length() > 0) {
            try (InputStream stream = searchSource.streamInput()) {
                builder.rawField(BODY_FIELD.getPreferredName(), stream);
            }
        }
        if (indicesOptions.equals(DEFAULT_INDICES_OPTIONS) == false) {
            builder.startObject(INDICES_OPTIONS_FIELD.getPreferredName());
            indicesOptions.toXContent(builder, params);
            builder.endObject();
        }
        if (template != null) {
            builder.field(TEMPLATE_FIELD.getPreferredName(), template);
        }
        return builder.endObject();
    }

    /**
     * Reads a new watcher search request instance for the specified parser.
     */
    public static WatcherSearchTemplateRequest fromXContent(XContentParser parser, SearchType searchType) throws IOException {
        List<String> indices = new ArrayList<>();
        IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;
        BytesReference searchSource = null;
        Script template = null;
        boolean totalHitsAsInt = true;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (INDICES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            indices.add(parser.textOrNull());
                        } else {
                            throw new ElasticsearchParseException(
                                "could not read search request. expected string values in ["
                                    + currentFieldName
                                    + "] field, but instead found ["
                                    + token
                                    + "]"
                            );
                        }
                    }
                } else {
                    throw new ElasticsearchParseException(
                        "could not read search request. unexpected array field [" + currentFieldName + "]"
                    );
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (BODY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                        builder.copyCurrentStructure(parser);
                        searchSource = BytesReference.bytes(builder);
                    }
                } else if (INDICES_OPTIONS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    indicesOptions = IndicesOptions.fromXContent(parser, DEFAULT_INDICES_OPTIONS);
                } else if (TEMPLATE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    template = Script.parse(parser, Script.DEFAULT_TEMPLATE_LANG);
                } else {
                    throw new ElasticsearchParseException(
                        "could not read search request. unexpected object field [" + currentFieldName + "]"
                    );
                }
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (INDICES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    String indicesStr = parser.text();
                    indices.addAll(Arrays.asList(Strings.delimitedListToStringArray(indicesStr, ",", " \t")));
                } else if (SEARCH_TYPE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    searchType = SearchType.fromString(parser.text().toLowerCase(Locale.ROOT));
                } else if (REST_TOTAL_HITS_AS_INT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    totalHitsAsInt = parser.booleanValue();
                } else {
                    throw new ElasticsearchParseException(
                        "could not read search request. unexpected string field [" + currentFieldName + "]"
                    );
                }
            } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                if (REST_TOTAL_HITS_AS_INT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    totalHitsAsInt = parser.booleanValue();
                } else {
                    throw new ElasticsearchParseException(
                        "could not read search request. unexpected boolean field [" + currentFieldName + "]"
                    );
                }
            } else {
                throw new ElasticsearchParseException("could not read search request. unexpected token [" + token + "]");
            }
        }

        if (searchSource == null) {
            searchSource = BytesArray.EMPTY;
        }

        WatcherSearchTemplateRequest request = new WatcherSearchTemplateRequest(
            indices.toArray(new String[0]),
            searchType,
            indicesOptions,
            searchSource,
            template
        );
        request.setRestTotalHitsAsInt(totalHitsAsInt);
        return request;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WatcherSearchTemplateRequest other = (WatcherSearchTemplateRequest) o;
        return Arrays.equals(indices, other.indices)
            && Objects.equals(searchType, other.searchType)
            && Objects.equals(indicesOptions, other.indicesOptions)
            && Objects.equals(searchSource, other.searchSource)
            && Objects.equals(template, other.template)
            && Objects.equals(restTotalHitsAsInt, other.restTotalHitsAsInt);

    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(indices), searchType, indicesOptions, searchSource, template, restTotalHitsAsInt);
    }

    private static final ParseField INDICES_FIELD = new ParseField("indices");
    private static final ParseField BODY_FIELD = new ParseField("body");
    private static final ParseField SEARCH_TYPE_FIELD = new ParseField("search_type");
    private static final ParseField INDICES_OPTIONS_FIELD = new ParseField("indices_options");
    private static final ParseField TEMPLATE_FIELD = new ParseField("template");
    private static final ParseField REST_TOTAL_HITS_AS_INT_FIELD = new ParseField("rest_total_hits_as_int");

    public static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.lenientExpandOpen();
}
