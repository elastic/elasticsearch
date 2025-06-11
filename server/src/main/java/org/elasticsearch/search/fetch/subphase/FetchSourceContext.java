/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.search.lookup.SourceFilter;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Context used to fetch the {@code _source}.
 */
public class FetchSourceContext implements Writeable, ToXContentObject {

    public static final ParseField EXCLUDE_VECTORS_FIELD = new ParseField("exclude_vectors");
    public static final ParseField INCLUDES_FIELD = new ParseField("includes", "include");
    public static final ParseField EXCLUDES_FIELD = new ParseField("excludes", "exclude");

    public static final FetchSourceContext FETCH_SOURCE = new FetchSourceContext(true, null, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY);
    public static final FetchSourceContext DO_NOT_FETCH_SOURCE = new FetchSourceContext(
        false,
        null,
        Strings.EMPTY_ARRAY,
        Strings.EMPTY_ARRAY
    );
    private final boolean fetchSource;
    private final String[] includes;
    private final String[] excludes;
    private final Boolean excludeVectors;

    public static FetchSourceContext of(boolean fetchSource) {
        return fetchSource ? FETCH_SOURCE : DO_NOT_FETCH_SOURCE;
    }

    public static FetchSourceContext of(boolean fetchSource, @Nullable String[] includes, @Nullable String[] excludes) {
        return of(fetchSource, null, includes, excludes);
    }

    public static FetchSourceContext of(
        boolean fetchSource,
        Boolean excludeVectors,
        @Nullable String[] includes,
        @Nullable String[] excludes
    ) {
        if (excludeVectors == null && (includes == null || includes.length == 0) && (excludes == null || excludes.length == 0)) {
            return of(fetchSource);
        }
        return new FetchSourceContext(fetchSource, excludeVectors, includes, excludes);
    }

    private FetchSourceContext(boolean fetchSource, Boolean excludeVectors, @Nullable String[] includes, @Nullable String[] excludes) {
        this.fetchSource = fetchSource;
        this.excludeVectors = excludeVectors;
        this.includes = includes == null ? Strings.EMPTY_ARRAY : includes;
        this.excludes = excludes == null ? Strings.EMPTY_ARRAY : excludes;
    }

    public static FetchSourceContext readFrom(StreamInput in) throws IOException {
        final boolean fetchSource = in.readBoolean();
        final Boolean excludeVectors = isVersionCompatibleWithExcludeVectors(in.getTransportVersion()) ? in.readOptionalBoolean() : null;
        final String[] includes = in.readStringArray();
        final String[] excludes = in.readStringArray();
        return of(fetchSource, excludeVectors, includes, excludes);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(fetchSource);
        if (isVersionCompatibleWithExcludeVectors(out.getTransportVersion())) {
            out.writeOptionalBoolean(excludeVectors);
        }
        out.writeStringArray(includes);
        out.writeStringArray(excludes);
    }

    private static boolean isVersionCompatibleWithExcludeVectors(TransportVersion version) {
        return version.isPatchFrom(TransportVersions.SEARCH_SOURCE_EXCLUDE_VECTORS_PARAM_8_19)
            || version.onOrAfter(TransportVersions.SEARCH_SOURCE_EXCLUDE_VECTORS_PARAM);
    }

    public boolean fetchSource() {
        return this.fetchSource;
    }

    public Boolean excludeVectors() {
        return this.excludeVectors;
    }

    public String[] includes() {
        return this.includes;
    }

    public String[] excludes() {
        return this.excludes;
    }

    private boolean hasFilter() {
        return this.includes.length > 0 || this.excludes.length > 0;
    }

    /**
     * Returns a {@link SourceFilter} if filtering is enabled, {@code null} otherwise.
     */
    public SourceFilter filter() {
        return hasFilter() ? new SourceFilter(includes, excludes) : null;
    }

    public static FetchSourceContext parseFromRestRequest(RestRequest request) {
        Boolean fetchSource = null;
        String[] sourceExcludes = null;
        String[] sourceIncludes = null;

        String source = request.param("_source");
        if (source != null) {
            if (Booleans.isTrue(source)) {
                fetchSource = true;
            } else if (Booleans.isFalse(source)) {
                fetchSource = false;
            } else {
                sourceIncludes = Strings.splitStringByCommaToArray(source);
            }
        }

        String sIncludes = request.param("_source_includes");
        if (sIncludes != null) {
            sourceIncludes = Strings.splitStringByCommaToArray(sIncludes);
        }

        String sExcludes = request.param("_source_excludes");
        if (sExcludes != null) {
            sourceExcludes = Strings.splitStringByCommaToArray(sExcludes);
        }

        if (fetchSource != null || sourceIncludes != null || sourceExcludes != null) {
            return FetchSourceContext.of(fetchSource == null || fetchSource, sourceIncludes, sourceExcludes);
        }
        return null;
    }

    public static FetchSourceContext fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }

        XContentParser.Token token = parser.currentToken();
        boolean fetchSource = true;
        Boolean excludeVectors = null;
        String[] includes = Strings.EMPTY_ARRAY;
        String[] excludes = Strings.EMPTY_ARRAY;
        if (token == XContentParser.Token.VALUE_BOOLEAN) {
            fetchSource = parser.booleanValue();
        } else if (token == XContentParser.Token.VALUE_STRING) {
            includes = new String[] { parser.text() };
        } else if (token == XContentParser.Token.START_ARRAY) {
            ArrayList<String> list = new ArrayList<>();
            while ((parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                list.add(parser.text());
            }
            includes = list.toArray(Strings.EMPTY_ARRAY);
        } else if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if (INCLUDES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        includes = parseStringArray(parser, currentFieldName);
                    } else if (EXCLUDES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        excludes = parseStringArray(parser, currentFieldName);
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation()
                        );
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (INCLUDES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        includes = new String[] { parser.text() };
                    } else if (EXCLUDES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        excludes = new String[] { parser.text() };
                    } else if (EXCLUDE_VECTORS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        excludeVectors = parser.booleanValue();
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation()
                        );
                    }
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    if (EXCLUDE_VECTORS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        excludeVectors = parser.booleanValue();
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation()
                        );
                    }
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + currentFieldName + "].",
                        parser.getTokenLocation()
                    );
                }
            }
        } else {
            throw new ParsingException(
                parser.getTokenLocation(),
                "Expected one of ["
                    + XContentParser.Token.VALUE_BOOLEAN
                    + ", "
                    + XContentParser.Token.VALUE_STRING
                    + ", "
                    + XContentParser.Token.START_ARRAY
                    + ", "
                    + XContentParser.Token.START_OBJECT
                    + "] but found ["
                    + token
                    + "]",
                parser.getTokenLocation()
            );
        }
        return FetchSourceContext.of(fetchSource, excludeVectors, includes, excludes);
    }

    private static String[] parseStringArray(XContentParser parser, String currentFieldName) throws IOException {
        XContentParser.Token token;
        String[] excludes;
        List<String> excludesList = new ArrayList<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.VALUE_STRING) {
                excludesList.add(parser.text());
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Unknown key for a " + token + " in [" + currentFieldName + "].",
                    parser.getTokenLocation()
                );
            }
        }
        excludes = excludesList.toArray(Strings.EMPTY_ARRAY);
        return excludes;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (fetchSource) {
            builder.startObject();
            if (excludeVectors != null) {
                builder.field(EXCLUDE_VECTORS_FIELD.getPreferredName(), excludeVectors);
            }
            builder.array(INCLUDES_FIELD.getPreferredName(), includes);
            builder.array(EXCLUDES_FIELD.getPreferredName(), excludes);
            builder.endObject();
        } else {
            builder.value(false);
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FetchSourceContext that = (FetchSourceContext) o;

        if (fetchSource != that.fetchSource) return false;
        if (excludeVectors != that.excludeVectors) return false;
        if (Arrays.equals(excludes, that.excludes) == false) return false;
        if (Arrays.equals(includes, that.includes) == false) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(fetchSource, excludeVectors);
        result = 31 * result + Arrays.hashCode(includes);
        result = 31 * result + Arrays.hashCode(excludes);
        return result;
    }
}
