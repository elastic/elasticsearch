/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.mapper.InferenceFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.mapper.vectors.SparseVectorFieldMapper;
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
import java.util.Map;
import java.util.Objects;

/**
 * Context used to fetch the {@code _source}.
 */
public class FetchSourceContext implements Writeable, ToXContentObject {
    public static final NodeFeature HIDE_VECTORS_IN_SOURCE = new NodeFeature("search.hide_vectors_in_source");

    public static final ParseField INCLUDES_FIELD = new ParseField("includes", "include");
    public static final ParseField EXCLUDES_FIELD = new ParseField("excludes", "exclude");
    public static final ParseField INCLUDE_VECTORS = new ParseField("include_vectors");

    public static final Boolean DEFAULT_INCLUDE_VECTORS = null;
    public static final FetchSourceContext FETCH_SOURCE = new FetchSourceContext(
        true,
        Strings.EMPTY_ARRAY,
        Strings.EMPTY_ARRAY,
        DEFAULT_INCLUDE_VECTORS
    );
    public static final FetchSourceContext FETCH_SOURCE_WITH_VECTORS = new FetchSourceContext(
        true,
        Strings.EMPTY_ARRAY,
        Strings.EMPTY_ARRAY,
        true
    );
    public static final FetchSourceContext DO_NOT_FETCH_SOURCE = new FetchSourceContext(
        false,
        Strings.EMPTY_ARRAY,
        Strings.EMPTY_ARRAY,
        DEFAULT_INCLUDE_VECTORS
    );

    private final boolean fetchSource;
    private final String[] includes;
    private final String[] excludes;
    private final Boolean includeVectors;

    public static FetchSourceContext of(boolean fetchSource) {
        return fetchSource ? FETCH_SOURCE : DO_NOT_FETCH_SOURCE;
    }

    public static FetchSourceContext of(boolean fetchSource, @Nullable String[] includes, @Nullable String[] excludes) {
        if ((includes == null || includes.length == 0) && (excludes == null || excludes.length == 0)) {
            return of(fetchSource);
        }
        return new FetchSourceContext(fetchSource, includes, excludes, DEFAULT_INCLUDE_VECTORS);
    }

    public static FetchSourceContext of(
        boolean fetchSource,
        @Nullable String[] includes,
        @Nullable String[] excludes,
        @Nullable Boolean includeVectors
    ) {
        if ((includes == null || includes.length == 0)
            && (excludes == null || excludes.length == 0)
            && includeVectors == DEFAULT_INCLUDE_VECTORS) {
            return of(fetchSource);
        }
        return new FetchSourceContext(fetchSource, includes, excludes, includeVectors);
    }

    public static FetchSourceContext readFrom(StreamInput in) throws IOException {
        final boolean fetchSource = in.readBoolean();
        final String[] includes = in.readStringArray();
        final String[] excludes = in.readStringArray();
        final Boolean includeVectors;
        if (in.getTransportVersion().onOrAfter(TransportVersions.HIDE_VECTORS_IN_SOURCE)) {
            includeVectors = in.readOptionalBoolean();
        } else {
            includeVectors = true;
        }
        return of(fetchSource, includes, excludes, includeVectors);
    }

    private FetchSourceContext(
        boolean fetchSource,
        @Nullable String[] includes,
        @Nullable String[] excludes,
        @Nullable Boolean includeVectors
    ) {
        this.fetchSource = fetchSource;
        this.includes = includes == null ? Strings.EMPTY_ARRAY : includes;
        this.excludes = excludes == null ? Strings.EMPTY_ARRAY : excludes;
        this.includeVectors = includeVectors;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(fetchSource);
        out.writeStringArray(includes);
        out.writeStringArray(excludes);
        if (out.getTransportVersion().onOrAfter(TransportVersions.HIDE_VECTORS_IN_SOURCE)) {
            out.writeOptionalBoolean(includeVectors);
        }
    }

    public boolean fetchSource() {
        return this.fetchSource;
    }

    public String[] includes() {
        return this.includes;
    }

    public String[] excludes() {
        return this.excludes;
    }

    public Boolean includeVectors() {
        return this.includeVectors;
    }

    public boolean filterVectorFields() {
        return this.includeVectors == null || this.includeVectors == false;
    }

    public boolean hasFilter() {
        return this.includes.length > 0 || this.excludes.length > 0;
    }

    public SourceFilter filter() {
        return filter(null);
    }

    public SourceFilter filter(MappingLookup mappingLookup) {
        if (filterVectorFields() == false) {
            return new SourceFilter(includes, excludes);
        } else {
            if (mappingLookup == null) {
                throw new IllegalStateException("mappingLookup must not be null when filtering vectors");
            }

            String[] excludeFields = excludes();
            List<String> inferenceFieldNames = new ArrayList<>(mappingLookup.inferenceFields().size());
            for (String inferenceField : mappingLookup.inferenceFields().keySet()) {
                Mapper mapper = mappingLookup.getMapper(inferenceField);
                if (mapper instanceof InferenceFieldMapper == false) {
                    throw new IllegalStateException("Mapper for field [" + inferenceField + "] is not an inference field mapper");
                }

                InferenceFieldMapper inferenceFieldMapper = (InferenceFieldMapper) mapper;
                inferenceFieldNames.add(inferenceFieldMapper.getInferenceFieldName());
            }
            excludeFields = ArrayUtils.concat(excludeFields, inferenceFieldNames.toArray(new String[0]));

            if (includeVectors != null) {
                String[] denseVectors = mappingLookup.getFullNameToFieldType()
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue() instanceof DenseVectorFieldMapper.DenseVectorFieldType)
                    .map(Map.Entry::getKey)
                    .toArray(String[]::new);
                excludeFields = ArrayUtils.concat(excludeFields, denseVectors);

                String[] sparseVectors = mappingLookup.getFullNameToFieldType()
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue() instanceof SparseVectorFieldMapper.SparseVectorFieldType)
                    .map(Map.Entry::getKey)
                    .toArray(String[]::new);
                excludeFields = ArrayUtils.concat(excludeFields, sparseVectors);
            }

            return new SourceFilter(includes, excludeFields);
        }
    }

    public static FetchSourceContext parseFromRestRequest(RestRequest request) {
        Boolean fetchSource = null;
        String[] sourceExcludes = null;
        String[] sourceIncludes = null;
        Boolean includeVectors = null;

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

        String sIncludeVectors = request.param("_source_include_vectors");
        if (sIncludeVectors != null) {
            includeVectors = Booleans.parseBoolean(sIncludeVectors);
        }

        if (fetchSource != null || sourceIncludes != null || sourceExcludes != null || includeVectors != null) {
            return FetchSourceContext.of(fetchSource == null || fetchSource, sourceIncludes, sourceExcludes, includeVectors);
        }
        return null;
    }

    public static FetchSourceContext fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }

        XContentParser.Token token = parser.currentToken();
        boolean fetchSource = true;
        String[] includes = Strings.EMPTY_ARRAY;
        String[] excludes = Strings.EMPTY_ARRAY;
        Boolean includeVectors = DEFAULT_INCLUDE_VECTORS;
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
                    } else if (INCLUDE_VECTORS.match(currentFieldName, parser.getDeprecationHandler())) {
                        includeVectors = parser.booleanValue();
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation()
                        );
                    }
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    if (INCLUDE_VECTORS.match(currentFieldName, parser.getDeprecationHandler())) {
                        includeVectors = parser.booleanValue();
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation()
                        );
                    }
                } else if (token == XContentParser.Token.VALUE_NULL) {
                    if (INCLUDE_VECTORS.match(currentFieldName, parser.getDeprecationHandler())) {
                        includeVectors = null;
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
        return FetchSourceContext.of(fetchSource, includes, excludes, includeVectors);
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
            builder.array(INCLUDES_FIELD.getPreferredName(), includes);
            builder.array(EXCLUDES_FIELD.getPreferredName(), excludes);
            if (includeVectors != null) {
                builder.field(INCLUDE_VECTORS.getPreferredName(), includeVectors);
            }
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
        if (Arrays.equals(excludes, that.excludes) == false) return false;
        if (Arrays.equals(includes, that.includes) == false) return false;
        if (includeVectors != that.includeVectors) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(fetchSource, includeVectors);
        result = 31 * result + Arrays.hashCode(includes);
        result = 31 * result + Arrays.hashCode(excludes);
        return result;
    }
}
