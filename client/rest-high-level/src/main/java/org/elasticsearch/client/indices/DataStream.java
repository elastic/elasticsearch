/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.indices;

import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public final class DataStream {

    private final String name;
    private final String timeStampField;
    private final List<String> indices;
    private final long generation;
    private final boolean hidden;
    private final boolean system;
    ClusterHealthStatus dataStreamStatus;
    @Nullable
    String indexTemplate;
    @Nullable
    String ilmPolicyName;
    @Nullable
    private final Map<String, Object> metadata;
    private final boolean allowCustomRouting;
    private final boolean replicated;

    public DataStream(
        String name,
        String timeStampField,
        List<String> indices,
        long generation,
        ClusterHealthStatus dataStreamStatus,
        @Nullable String indexTemplate,
        @Nullable String ilmPolicyName,
        @Nullable Map<String, Object> metadata,
        boolean hidden,
        boolean system,
        boolean allowCustomRouting,
        boolean replicated
    ) {
        this.name = name;
        this.timeStampField = timeStampField;
        this.indices = indices;
        this.generation = generation;
        this.dataStreamStatus = dataStreamStatus;
        this.indexTemplate = indexTemplate;
        this.ilmPolicyName = ilmPolicyName;
        this.metadata = metadata;
        this.hidden = hidden;
        this.system = system;
        this.allowCustomRouting = allowCustomRouting;
        this.replicated = replicated;
    }

    public String getName() {
        return name;
    }

    public String getTimeStampField() {
        return timeStampField;
    }

    public List<String> getIndices() {
        return indices;
    }

    public long getGeneration() {
        return generation;
    }

    public ClusterHealthStatus getDataStreamStatus() {
        return dataStreamStatus;
    }

    public String getIndexTemplate() {
        return indexTemplate;
    }

    public String getIlmPolicyName() {
        return ilmPolicyName;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public boolean isHidden() {
        return hidden;
    }

    public boolean isSystem() {
        return system;
    }

    public boolean allowsCustomRouting() {
        return allowCustomRouting;
    }

    public boolean isReplicated() {
        return replicated;
    }

    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final ParseField TIMESTAMP_FIELD_FIELD = new ParseField("timestamp_field");
    public static final ParseField INDICES_FIELD = new ParseField("indices");
    public static final ParseField GENERATION_FIELD = new ParseField("generation");
    public static final ParseField STATUS_FIELD = new ParseField("status");
    public static final ParseField INDEX_TEMPLATE_FIELD = new ParseField("template");
    public static final ParseField ILM_POLICY_FIELD = new ParseField("ilm_policy");
    public static final ParseField METADATA_FIELD = new ParseField("_meta");
    public static final ParseField HIDDEN_FIELD = new ParseField("hidden");
    public static final ParseField SYSTEM_FIELD = new ParseField("system");
    public static final ParseField ALLOW_CUSTOM_ROUTING = new ParseField("allow_custom_routing");
    public static final ParseField REPLICATED = new ParseField("replicated");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<DataStream, Void> PARSER = new ConstructingObjectParser<>("data_stream", args -> {
        String dataStreamName = (String) args[0];
        String timeStampField = (String) ((Map<?, ?>) args[1]).get("name");
        List<String> indices = ((List<Map<String, String>>) args[2]).stream().map(m -> m.get("index_name")).collect(Collectors.toList());
        Long generation = (Long) args[3];
        String statusStr = (String) args[4];
        ClusterHealthStatus status = ClusterHealthStatus.fromString(statusStr);
        String indexTemplate = (String) args[5];
        String ilmPolicy = (String) args[6];
        Map<String, Object> metadata = (Map<String, Object>) args[7];
        boolean hidden = args[8] != null && (boolean) args[8];
        boolean system = args[9] != null && (boolean) args[9];
        boolean allowCustomRouting = args[10] != null && (boolean) args[10];
        boolean replicated = args[11] != null && (boolean) args[11];
        return new DataStream(
            dataStreamName,
            timeStampField,
            indices,
            generation,
            status,
            indexTemplate,
            ilmPolicy,
            metadata,
            hidden,
            system,
            allowCustomRouting,
            replicated
        );
    });

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.map(), TIMESTAMP_FIELD_FIELD);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> p.mapStrings(), INDICES_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), GENERATION_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), STATUS_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), INDEX_TEMPLATE_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ILM_POLICY_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), METADATA_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), HIDDEN_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), SYSTEM_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ALLOW_CUSTOM_ROUTING);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), REPLICATED);
    }

    public static DataStream fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataStream that = (DataStream) o;
        return generation == that.generation
            && name.equals(that.name)
            && timeStampField.equals(that.timeStampField)
            && indices.equals(that.indices)
            && dataStreamStatus == that.dataStreamStatus
            && hidden == that.hidden
            && system == that.system
            && Objects.equals(indexTemplate, that.indexTemplate)
            && Objects.equals(ilmPolicyName, that.ilmPolicyName)
            && Objects.equals(metadata, that.metadata)
            && allowCustomRouting == that.allowCustomRouting
            && replicated == that.replicated;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            name,
            timeStampField,
            indices,
            generation,
            dataStreamStatus,
            indexTemplate,
            ilmPolicyName,
            metadata,
            hidden,
            system,
            allowCustomRouting,
            replicated
        );
    }
}
