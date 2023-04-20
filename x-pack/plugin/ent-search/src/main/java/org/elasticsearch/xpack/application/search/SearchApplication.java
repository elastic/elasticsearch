/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Search Application consists of:
 * <ul>
 *     <li>A name identifier</li>
 *     <li>A list of indices, which will be used for querying</li>
 *     <li>An {@link org.elasticsearch.xpack.application.analytics.AnalyticsCollection} identifier, where analytics will be stored</li>
 *     <li>A {@link SearchApplicationTemplate} that contains the template and default parameters used for querying
 *     the Search Application</li>
 * </ul>
 */
public class SearchApplication implements Writeable, ToXContentObject {

    private final String name;
    private final String[] indices;
    private final long updatedAtMillis;
    private final String analyticsCollectionName;
    private final SearchApplicationTemplate searchApplicationTemplate;

    /**
     * Public constructor.
     *
     * @param name                      The name of the search application.
     * @param indices                   The list of indices targeted by this search application.
     * @param analyticsCollectionName   The name of the associated analytics collection.
     * @param updatedAtMillis           Last updated time in milliseconds for the search application.
     * @param searchApplicationTemplate The search application template to be used on search
     */
    public SearchApplication(
        String name,
        String[] indices,
        @Nullable String analyticsCollectionName,
        long updatedAtMillis,
        @Nullable SearchApplicationTemplate searchApplicationTemplate
    ) {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("Search Application name cannot be null or blank");
        }
        this.name = name;

        Objects.requireNonNull(indices, "Search Application indices cannot be null");
        this.indices = indices.clone();
        Arrays.sort(this.indices);

        this.analyticsCollectionName = analyticsCollectionName;
        this.updatedAtMillis = updatedAtMillis;
        this.searchApplicationTemplate = searchApplicationTemplate != null
            ? searchApplicationTemplate
            : SearchApplicationTemplate.DEFAULT_TEMPLATE;
    }

    public SearchApplication(StreamInput in) throws IOException {
        this.name = in.readString();
        this.indices = in.readStringArray();
        this.analyticsCollectionName = in.readOptionalString();
        this.updatedAtMillis = in.readLong();
        this.searchApplicationTemplate = in.readOptionalWriteable(SearchApplicationTemplate::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringArray(indices);
        out.writeOptionalString(analyticsCollectionName);
        out.writeLong(updatedAtMillis);
        out.writeOptionalWriteable(searchApplicationTemplate);
    }

    private static final ConstructingObjectParser<SearchApplication, String> PARSER = new ConstructingObjectParser<>(
        "search_application",
        false,
        (params, resourceName) -> {
            final String name = (String) params[0];
            // If name is provided, check that it matches the resource name. We don't want it to be updatable
            if (name != null) {
                if (name.equals(resourceName) == false) {
                    throw new IllegalArgumentException(
                        "Search Application name [" + name + "] does not match the resource name: [" + resourceName + "]"
                    );
                }
            }
            @SuppressWarnings("unchecked")
            final String[] indices = ((List<String>) params[1]).toArray(String[]::new);
            final String analyticsCollectionName = (String) params[2];
            final Long maybeUpdatedAtMillis = (Long) params[3];
            long updatedAtMillis = (maybeUpdatedAtMillis != null ? maybeUpdatedAtMillis : System.currentTimeMillis());
            final SearchApplicationTemplate template = (SearchApplicationTemplate) params[4];

            SearchApplication newApp = new SearchApplication(resourceName, indices, analyticsCollectionName, updatedAtMillis, template);
            return newApp;
        }
    );

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField INDICES_FIELD = new ParseField("indices");
    public static final ParseField ANALYTICS_COLLECTION_NAME_FIELD = new ParseField("analytics_collection_name");
    public static final ParseField TEMPLATE_FIELD = new ParseField("template");
    public static final ParseField TEMPLATE_SCRIPT_FIELD = new ParseField("script");
    public static final ParseField UPDATED_AT_MILLIS_FIELD = new ParseField("updated_at_millis");
    public static final ParseField BINARY_CONTENT_FIELD = new ParseField("binary_content");

    static {
        PARSER.declareStringOrNull(optionalConstructorArg(), NAME_FIELD);
        PARSER.declareStringArray(constructorArg(), INDICES_FIELD);
        PARSER.declareStringOrNull(optionalConstructorArg(), ANALYTICS_COLLECTION_NAME_FIELD);
        PARSER.declareLong(optionalConstructorArg(), UPDATED_AT_MILLIS_FIELD);
        PARSER.declareObjectOrNull(optionalConstructorArg(), (p, c) -> SearchApplicationTemplate.parse(p), null, TEMPLATE_FIELD);
    }

    /**
     * Parses an {@link SearchApplication} from its {@param xContentType} representation in bytes.
     *
     * @param resourceName The name of the resource (must match the {@link SearchApplication} name).
     * @param source The bytes that represents the {@link SearchApplication}.
     * @param xContentType The format of the representation.
     *
     * @return The parsed {@link SearchApplication}.
     */
    public static SearchApplication fromXContentBytes(String resourceName, BytesReference source, XContentType xContentType) {
        try (XContentParser parser = XContentHelper.createParser(XContentParserConfiguration.EMPTY, source, xContentType)) {
            return SearchApplication.fromXContent(resourceName, parser);
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse: " + source.utf8ToString(), e);
        }
    }

    /**
     * Parses an {@link SearchApplication} through the provided {@param parser}.
     *
     * @param resourceName The name of the resource (must match the {@link SearchApplication} name).
     * @param parser The {@link XContentType} parser.
     *
     * @return The parsed {@link SearchApplication}.
     */
    public static SearchApplication fromXContent(String resourceName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, resourceName);
    }

    /**
     * Converts the {@link SearchApplication} to XContent.
     *
     * @return The {@link XContentBuilder} containing the serialized {@link SearchApplication}.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME_FIELD.getPreferredName(), name);
        builder.field(INDICES_FIELD.getPreferredName(), indices);
        if (analyticsCollectionName != null) {
            builder.field(ANALYTICS_COLLECTION_NAME_FIELD.getPreferredName(), analyticsCollectionName);
        }
        builder.field(UPDATED_AT_MILLIS_FIELD.getPreferredName(), updatedAtMillis);
        builder.field(TEMPLATE_FIELD.getPreferredName(), searchApplicationTemplate);
        builder.endObject();
        return builder;
    }

    /**
     * Returns the name of the {@link SearchApplication}.
     *
     * @return The name of the {@link SearchApplication}.
     */
    public String name() {
        return name;
    }

    /**
     * Returns the list of indices targeted by the {@link SearchApplication}.
     *
     * @return The list of indices targeted by the {@link SearchApplication}.
     */
    public String[] indices() {
        return indices;
    }

    /**
     * Returns the name of the analytics collection linked with this {@link SearchApplication}.
     *
     * @return The analytics collection name.
     */
    public @Nullable String analyticsCollectionName() {
        return analyticsCollectionName;
    }

    /**
     * Returns the timestamp in milliseconds that this {@link SearchApplication} was last modified.
     *
     * @return The last updated timestamp in milliseconds.
     */
    public long updatedAtMillis() {
        return updatedAtMillis;
    }

    public @Nullable SearchApplicationTemplate searchApplicationTemplate() {
        return searchApplicationTemplate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchApplication app = (SearchApplication) o;
        return name.equals(app.name)
            && Arrays.equals(indices, app.indices)
            && Objects.equals(analyticsCollectionName, app.analyticsCollectionName)
            && updatedAtMillis == app.updatedAtMillis()
            && Objects.equals(searchApplicationTemplate, app.searchApplicationTemplate);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(name, analyticsCollectionName, updatedAtMillis, searchApplicationTemplate);
        result = 31 * result + Arrays.hashCode(indices);
        return result;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * Returns the merged {@link SearchApplication} from the current state and the provided {@param update}.
     * This function returns the current instance if the update is a noop.
     *
     * @param update The source of the update represented in bytes.
     * @param xContentType The format of the bytes.
     * @param bigArrays The {@link BigArrays} to use to recycle bytes array.
     *
     * @return The merged {@link SearchApplication}.
     */
    SearchApplication merge(BytesReference update, XContentType xContentType, BigArrays bigArrays) throws IOException {
        final Tuple<XContentType, Map<String, Object>> sourceAndContent;
        try (ReleasableBytesStreamOutput sourceBuffer = new ReleasableBytesStreamOutput(0, bigArrays.withCircuitBreaking())) {
            try (XContentBuilder builder = XContentFactory.jsonBuilder(sourceBuffer)) {
                toXContent(builder, EMPTY_PARAMS);
            }
            sourceAndContent = XContentHelper.convertToMap(sourceBuffer.bytes(), true, XContentType.JSON);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        final Tuple<XContentType, Map<String, Object>> updateAndContent = XContentHelper.convertToMap(update, true, xContentType);
        final Map<String, Object> newSourceAsMap = new HashMap<>(sourceAndContent.v2());
        final boolean noop = XContentHelper.update(newSourceAsMap, updateAndContent.v2(), true) == false;
        if (noop) {
            return this;
        }

        try (ReleasableBytesStreamOutput newSourceBuffer = new ReleasableBytesStreamOutput(0, bigArrays.withCircuitBreaking())) {
            try (XContentBuilder builder = XContentFactory.jsonBuilder(newSourceBuffer)) {
                builder.value(newSourceAsMap);
            }
            return SearchApplication.fromXContentBytes(name, newSourceBuffer.bytes(), XContentType.JSON);
        }
    }
}
