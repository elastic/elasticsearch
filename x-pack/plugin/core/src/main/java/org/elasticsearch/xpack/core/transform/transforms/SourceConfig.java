/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.utils.ExceptionsHelper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SourceConfig implements Writeable, ToXContentObject {

    public static final ParseField QUERY = new ParseField("query");
    public static final ParseField INDEX = new ParseField("index");
    public static final ParseField PROJECT_ROUTING = new ParseField("project_routing");

    public static final ConstructingObjectParser<SourceConfig, TransformParsingContext> STRICT_PARSER = createParser(false);
    public static final ConstructingObjectParser<SourceConfig, TransformParsingContext> LENIENT_PARSER = createParser(true);

    static final TransportVersion TRANSFORM_PROJECT_ROUTING = TransportVersion.fromName("transform_project_routing");
    static final TransportVersion TRANSFORM_INDICES_OPTIONS = TransportVersion.fromName("transform_indices_options");

    private static final IndicesOptions DEFAULT_INDICES_OPTIONS = IndicesOptions.LENIENT_EXPAND_OPEN;

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<SourceConfig, TransformParsingContext> createParser(boolean lenient) {
        ConstructingObjectParser<SourceConfig, TransformParsingContext> parser = new ConstructingObjectParser<>(
            "data_frame_config_source",
            lenient,
            (args, context) -> {
                String[] index = ((List<String>) args[0]).toArray(new String[0]);
                // default handling: if the user does not specify a query, we default to match_all
                QueryConfig queryConfig = args[1] == null ? QueryConfig.matchAll() : (QueryConfig) args[1];
                Map<String, Object> runtimeMappings = args[2] == null ? Collections.emptyMap() : (Map<String, Object>) args[2];
                var projectRouting = (String) args[3];

                return new SourceConfig(index, queryConfig, runtimeMappings, context.getDefaultIndicesOptions(), projectRouting);
            }
        );
        parser.declareStringArray(constructorArg(), INDEX);
        parser.declareObject(optionalConstructorArg(), (p, c) -> QueryConfig.fromXContent(p, lenient), QUERY);
        parser.declareObject(optionalConstructorArg(), (p, c) -> p.map(), SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD);
        parser.declareString(optionalConstructorArg(), PROJECT_ROUTING);
        return parser;
    }

    private final String[] index;
    private final QueryConfig queryConfig;
    private final Map<String, Object> runtimeMappings;
    private final IndicesOptions indicesOptions;
    @Nullable
    private final String projectRouting;

    /**
     * Create a new SourceConfig for the provided indices.
     *
     * {@link QueryConfig} defaults to a MatchAll query.
     *
     * @param index Any number of indices. At least one non-null, non-empty, index should be provided
     */
    public SourceConfig(String... index) {
        this(index, QueryConfig.matchAll(), Collections.emptyMap(), DEFAULT_INDICES_OPTIONS, null);
    }

    /**
     * Create a new SourceConfig for the provided indices, from which data is gathered with the provided {@link QueryConfig}
     *
     * @param index Any number of indices. At least one non-null, non-empty, index should be provided
     * @param queryConfig A QueryConfig object that contains the desired query, needs to be non-null
     * @param runtimeMappings Search-time runtime fields that can be used by the transform
     * @param projectRouting filter for cross-project search, null when cross-project search is disabled
     */
    public SourceConfig(String[] index, QueryConfig queryConfig, Map<String, Object> runtimeMappings, @Nullable String projectRouting) {
        this(index, queryConfig, runtimeMappings, DEFAULT_INDICES_OPTIONS, projectRouting);
    }

    /**
     * Create a new SourceConfig for the provided indices, from which data is gathered with the provided {@link QueryConfig}
     *
     * @param index Any number of indices. At least one non-null, non-empty, index should be provided
     * @param queryConfig A QueryConfig object that contains the desired query, needs to be non-null
     * @param runtimeMappings Search-time runtime fields that can be used by the transform
     * @param indicesOptions IndicesOptions to use when searching the index
     * @param projectRouting filter for cross-project search, null when cross-project search is disabled
     */
    public SourceConfig(
        String[] index,
        QueryConfig queryConfig,
        Map<String, Object> runtimeMappings,
        IndicesOptions indicesOptions,
        @Nullable String projectRouting
    ) {
        this.index = extractIndices(ExceptionsHelper.requireNonNull(index, INDEX.getPreferredName()));
        this.queryConfig = ExceptionsHelper.requireNonNull(queryConfig, QUERY.getPreferredName());
        this.runtimeMappings = Collections.unmodifiableMap(
            ExceptionsHelper.requireNonNull(runtimeMappings, SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD.getPreferredName())
        );
        this.indicesOptions = Objects.requireNonNull(indicesOptions);
        this.projectRouting = projectRouting;
    }

    /**
     * Extracts all index names or index patterns from the given array of strings.
     *
     * @param index array of indices (may contain comma-separated index names or index patterns)
     * @return array of indices without comma-separated index names or index patterns
     */
    private static String[] extractIndices(String[] index) {
        if (index.length == 0) {
            throw new IllegalArgumentException("must specify at least one index");
        }
        if (Arrays.stream(index).anyMatch(Strings::isNullOrEmpty)) {
            throw new IllegalArgumentException("all indices need to be non-null and non-empty");
        }
        return Arrays.stream(index)
            .map(commaSeparatedIndices -> commaSeparatedIndices.split(","))
            .flatMap(Arrays::stream)
            .toArray(String[]::new);
    }

    public SourceConfig(final StreamInput in) throws IOException {
        index = in.readStringArray();
        queryConfig = new QueryConfig(in);
        runtimeMappings = in.readGenericMap();
        projectRouting = in.getTransportVersion().supports(TRANSFORM_PROJECT_ROUTING) ? in.readOptionalString() : null;
        indicesOptions = in.getTransportVersion().supports(TRANSFORM_INDICES_OPTIONS)
            ? IndicesOptions.readIndicesOptions(in)
            : DEFAULT_INDICES_OPTIONS;
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
        return getRuntimeMappings().entrySet()
            .stream()
            .filter(e -> e.getValue() instanceof Map<?, ?> && ((Map<?, ?>) e.getValue()).containsKey("script"))
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Nullable
    public String getProjectRouting() {
        return projectRouting;
    }

    public IndicesOptions indicesOptions() {
        return indicesOptions;
    }

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        return queryConfig.validate(validationException);
    }

    public void checkForDeprecations(String id, NamedXContentRegistry namedXContentRegistry, Consumer<DeprecationIssue> onDeprecation) {
        queryConfig.checkForDeprecations(id, namedXContentRegistry, onDeprecation);
    }

    public boolean requiresRemoteCluster() {
        return Arrays.stream(index).anyMatch(RemoteClusterLicenseChecker::isRemoteIndex);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(index);
        queryConfig.writeTo(out);
        out.writeGenericMap(runtimeMappings);
        if (out.getTransportVersion().supports(TRANSFORM_PROJECT_ROUTING)) {
            out.writeOptionalString(projectRouting);
        }
        if (out.getTransportVersion().supports(TRANSFORM_INDICES_OPTIONS)) {
            indicesOptions.writeIndicesOptions(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.array(INDEX.getPreferredName(), index);
        if (params.paramAsBoolean(TransformField.EXCLUDE_GENERATED, false) == false) {
            builder.field(QUERY.getPreferredName(), queryConfig);
        } else if (queryConfig.equals(QueryConfig.matchAll()) == false) {
            builder.field(QUERY.getPreferredName(), queryConfig);
        }
        if (runtimeMappings.isEmpty() == false) {
            builder.field(SearchSourceBuilder.RUNTIME_MAPPINGS_FIELD.getPreferredName(), runtimeMappings);
        }
        if (projectRouting != null) {
            builder.field(PROJECT_ROUTING.getPreferredName(), projectRouting);
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
            && Objects.equals(runtimeMappings, that.runtimeMappings)
            && Objects.equals(projectRouting, that.projectRouting)
            && Objects.equals(indicesOptions, that.indicesOptions);
    }

    @Override
    public int hashCode() {
        // Using Arrays.hashCode as Objects.hash does not deeply hash nested arrays. Since we are doing Array.equals, this is necessary
        int indexArrayHash = Arrays.hashCode(index);
        return Objects.hash(indexArrayHash, queryConfig, runtimeMappings, projectRouting, indicesOptions);
    }

    public static SourceConfig fromXContent(final XContentParser parser, boolean lenient, TransformParsingContext context)
        throws IOException {
        return lenient ? LENIENT_PARSER.apply(parser, context) : STRICT_PARSER.apply(parser, context);
    }
}
