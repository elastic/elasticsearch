/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;


public class SourceConfig implements Writeable, ToXContentObject {

    public static final ParseField QUERY = new ParseField("query");
    public static final ParseField INDEX = new ParseField("index");

    public static final ConstructingObjectParser<SourceConfig, Void> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<SourceConfig, Void> LENIENT_PARSER = createParser(true);

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<SourceConfig, Void> createParser(boolean lenient) {
        ConstructingObjectParser<SourceConfig, Void> parser = new ConstructingObjectParser<>("data_frame_config_source",
            lenient,
            args -> {
                String[] index = ((List<String>)args[0]).toArray(new String[0]);
                // default handling: if the user does not specify a query, we default to match_all
                QueryConfig queryConfig = args[1] == null ? QueryConfig.matchAll() : (QueryConfig) args[1];
                Map<String, Object> runtimeMappings = args[2] == null ? Collections.emptyMap() : (Map<String, Object>) args[2];
                return new SourceConfig(index, queryConfig, runtimeMappings);
            });
        parser.declareStringArray(constructorArg(), INDEX);
        parser.declareObject(optionalConstructorArg(), (p, c) -> QueryConfig.fromXContent(p, lenient), QUERY);
        parser.declareObject(optionalConstructorArg(), (p, c) -> p.map(), SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD);
        return parser;
    }

    private final String[] index;
    private final QueryConfig queryConfig;
    private final Map<String, Object> runtimeMappings;

    /**
     * Create a new SourceConfig for the provided indices.
     *
     * {@link QueryConfig} defaults to a MatchAll query.
     *
     * @param index Any number of indices. At least one non-null, non-empty, index should be provided
     */
    public SourceConfig(String... index) {
        this(index, QueryConfig.matchAll(), Collections.emptyMap());
    }

    /**
     * Create a new SourceConfig for the provided indices, from which data is gathered with the provided {@link QueryConfig}
     *
     * @param index Any number of indices. At least one non-null, non-empty, index should be provided
     * @param queryConfig A QueryConfig object that contains the desired query, needs to be non-null
     * @param runtimeMappings Search-time runtime fields that can be used by the transform
     */
    public SourceConfig(String[] index, QueryConfig queryConfig, Map<String, Object> runtimeMappings) {
        ExceptionsHelper.requireNonNull(index, INDEX.getPreferredName());
        if (index.length == 0) {
            throw new IllegalArgumentException("must specify at least one index");
        }
        if (Arrays.stream(index).anyMatch(Strings::isNullOrEmpty)) {
            throw new IllegalArgumentException("all indices need to be non-null and non-empty");
        }
        this.index = index;
        this.queryConfig = ExceptionsHelper.requireNonNull(queryConfig, QUERY.getPreferredName());
        this.runtimeMappings =
            Collections.unmodifiableMap(
                ExceptionsHelper.requireNonNull(runtimeMappings, SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD.getPreferredName()));
    }

    public SourceConfig(final StreamInput in) throws IOException {
        index = in.readStringArray();
        queryConfig = new QueryConfig(in);
        if (in.getVersion().onOrAfter(Version.V_7_12_0)) {
            runtimeMappings = in.readMap();
        } else {
            runtimeMappings = Collections.emptyMap();
        }
    }

    public String[] getIndex() {
        return index;
    }

    public QueryConfig getQueryConfig() {
        return queryConfig;
    }

    public Map<String, Object> getRuntimeMappings() {
        return runtimeMappings;
    }

    public Map<String, Object> getScriptBasedRuntimeMappings() {
        return getRuntimeMappings().entrySet().stream()
            .filter(e -> e.getValue() instanceof Map<?, ?> && ((Map<?, ?>) e.getValue()).containsKey("script"))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        return queryConfig.validate(validationException);
    }

    public boolean requiresRemoteCluster() {
        return Arrays.stream(index).anyMatch(RemoteClusterLicenseChecker::isRemoteIndex);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(index);
        queryConfig.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_7_12_0)) {
            out.writeMap(runtimeMappings);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.array(INDEX.getPreferredName(), index);
        if (params.paramAsBoolean(TransformField.EXCLUDE_GENERATED, false) == false) {
            builder.field(QUERY.getPreferredName(), queryConfig);
        } else if(queryConfig.equals(QueryConfig.matchAll()) == false) {
            builder.field(QUERY.getPreferredName(), queryConfig);
        }
        if (runtimeMappings.isEmpty() == false) {
            builder.field(SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD.getPreferredName(), runtimeMappings);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other == null || other.getClass() != getClass()) {
            return false;
        }

        SourceConfig that = (SourceConfig) other;
        return Arrays.equals(index, that.index)
            && Objects.equals(queryConfig, that.queryConfig)
            && Objects.equals(runtimeMappings, that.runtimeMappings);
    }

    @Override
    public int hashCode(){
        // Using Arrays.hashCode as Objects.hash does not deeply hash nested arrays. Since we are doing Array.equals, this is necessary
        int indexArrayHash = Arrays.hashCode(index);
        return Objects.hash(indexArrayHash, queryConfig, runtimeMappings);
    }

    public static SourceConfig fromXContent(final XContentParser parser, boolean lenient) throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
    }
}
