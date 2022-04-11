/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.cluster.metadata.DataStream.TimestampField.FIXED_TIMESTAMP_FIELD;

/**
 * An index template is comprised of a set of index patterns, an optional template, and a list of
 * ids corresponding to component templates that should be composed in order when creating a new
 * index.
 */
public class ComposableIndexTemplate implements SimpleDiffable<ComposableIndexTemplate>, ToXContentObject {
    private static final ParseField INDEX_PATTERNS = new ParseField("index_patterns");
    private static final ParseField TEMPLATE = new ParseField("template");
    private static final ParseField PRIORITY = new ParseField("priority");
    private static final ParseField COMPOSED_OF = new ParseField("composed_of");
    private static final ParseField VERSION = new ParseField("version");
    private static final ParseField METADATA = new ParseField("_meta");
    private static final ParseField DATA_STREAM = new ParseField("data_stream");
    private static final ParseField ALLOW_AUTO_CREATE = new ParseField("allow_auto_create");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ComposableIndexTemplate, Void> PARSER = new ConstructingObjectParser<>(
        "index_template",
        false,
        a -> new ComposableIndexTemplate(
            (List<String>) a[0],
            (Template) a[1],
            (List<String>) a[2],
            (Long) a[3],
            (Long) a[4],
            (Map<String, Object>) a[5],
            (DataStreamTemplate) a[6],
            (Boolean) a[7]
        )
    );

    static {
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), INDEX_PATTERNS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), Template.PARSER, TEMPLATE);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), COMPOSED_OF);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), PRIORITY);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), VERSION);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), METADATA);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), DataStreamTemplate.PARSER, DATA_STREAM);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ALLOW_AUTO_CREATE);
    }

    private final List<String> indexPatterns;
    @Nullable
    private final Template template;
    @Nullable
    private final List<String> componentTemplates;
    @Nullable
    private final Long priority;
    @Nullable
    private final Long version;
    @Nullable
    private final Map<String, Object> metadata;
    @Nullable
    private final DataStreamTemplate dataStreamTemplate;
    @Nullable
    private final Boolean allowAutoCreate;

    static Diff<ComposableIndexTemplate> readITV2DiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(ComposableIndexTemplate::new, in);
    }

    public static ComposableIndexTemplate parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public ComposableIndexTemplate(
        List<String> indexPatterns,
        @Nullable Template template,
        @Nullable List<String> componentTemplates,
        @Nullable Long priority,
        @Nullable Long version,
        @Nullable Map<String, Object> metadata
    ) {
        this(indexPatterns, template, componentTemplates, priority, version, metadata, null, null);
    }

    public ComposableIndexTemplate(
        List<String> indexPatterns,
        @Nullable Template template,
        @Nullable List<String> componentTemplates,
        @Nullable Long priority,
        @Nullable Long version,
        @Nullable Map<String, Object> metadata,
        @Nullable DataStreamTemplate dataStreamTemplate
    ) {
        this(indexPatterns, template, componentTemplates, priority, version, metadata, dataStreamTemplate, null);
    }

    public ComposableIndexTemplate(
        List<String> indexPatterns,
        @Nullable Template template,
        @Nullable List<String> componentTemplates,
        @Nullable Long priority,
        @Nullable Long version,
        @Nullable Map<String, Object> metadata,
        @Nullable DataStreamTemplate dataStreamTemplate,
        @Nullable Boolean allowAutoCreate
    ) {
        this.indexPatterns = indexPatterns;
        this.template = template;
        this.componentTemplates = componentTemplates;
        this.priority = priority;
        this.version = version;
        this.metadata = metadata;
        this.dataStreamTemplate = dataStreamTemplate;
        this.allowAutoCreate = allowAutoCreate;
    }

    public ComposableIndexTemplate(StreamInput in) throws IOException {
        this.indexPatterns = in.readStringList();
        if (in.readBoolean()) {
            this.template = new Template(in);
        } else {
            this.template = null;
        }
        this.componentTemplates = in.readOptionalStringList();
        this.priority = in.readOptionalVLong();
        this.version = in.readOptionalVLong();
        this.metadata = in.readMap();
        this.dataStreamTemplate = in.readOptionalWriteable(DataStreamTemplate::new);
        this.allowAutoCreate = in.readOptionalBoolean();
    }

    public List<String> indexPatterns() {
        return indexPatterns;
    }

    @Nullable
    public Template template() {
        return template;
    }

    public List<String> composedOf() {
        if (componentTemplates == null) {
            return List.of();
        }
        return componentTemplates;
    }

    @Nullable
    public Long priority() {
        return priority;
    }

    public long priorityOrZero() {
        if (priority == null) {
            return 0L;
        }
        return priority;
    }

    @Nullable
    public Long version() {
        return version;
    }

    @Nullable
    public Map<String, Object> metadata() {
        return metadata;
    }

    @Nullable
    public DataStreamTemplate getDataStreamTemplate() {
        return dataStreamTemplate;
    }

    @Nullable
    public Boolean getAllowAutoCreate() {
        return this.allowAutoCreate;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(this.indexPatterns);
        if (this.template == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            this.template.writeTo(out);
        }
        out.writeOptionalStringCollection(this.componentTemplates);
        out.writeOptionalVLong(this.priority);
        out.writeOptionalVLong(this.version);
        out.writeGenericMap(this.metadata);
        out.writeOptionalWriteable(dataStreamTemplate);
        out.writeOptionalBoolean(allowAutoCreate);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.stringListField(INDEX_PATTERNS.getPreferredName(), this.indexPatterns);
        if (this.template != null) {
            builder.field(TEMPLATE.getPreferredName(), this.template, params);
        }
        if (this.componentTemplates != null) {
            builder.stringListField(COMPOSED_OF.getPreferredName(), this.componentTemplates);
        }
        if (this.priority != null) {
            builder.field(PRIORITY.getPreferredName(), priority);
        }
        if (this.version != null) {
            builder.field(VERSION.getPreferredName(), version);
        }
        if (this.metadata != null) {
            builder.field(METADATA.getPreferredName(), metadata);
        }
        if (this.dataStreamTemplate != null) {
            builder.field(DATA_STREAM.getPreferredName(), dataStreamTemplate, params);
        }
        if (this.allowAutoCreate != null) {
            builder.field(ALLOW_AUTO_CREATE.getPreferredName(), allowAutoCreate);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            this.indexPatterns,
            this.template,
            this.componentTemplates,
            this.priority,
            this.version,
            this.metadata,
            this.dataStreamTemplate,
            this.allowAutoCreate
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ComposableIndexTemplate other = (ComposableIndexTemplate) obj;
        return Objects.equals(this.indexPatterns, other.indexPatterns)
            && Objects.equals(this.template, other.template)
            && componentTemplatesEquals(this.componentTemplates, other.componentTemplates)
            && Objects.equals(this.priority, other.priority)
            && Objects.equals(this.version, other.version)
            && Objects.equals(this.metadata, other.metadata)
            && Objects.equals(this.dataStreamTemplate, other.dataStreamTemplate)
            && Objects.equals(this.allowAutoCreate, other.allowAutoCreate);
    }

    static boolean componentTemplatesEquals(List<String> c1, List<String> c2) {
        if (Objects.equals(c1, c2)) {
            return true;
        }
        if (c1 == null && c2.isEmpty()) {
            return true;
        }
        if (c2 == null && c1.isEmpty()) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class DataStreamTemplate implements Writeable, ToXContentObject {

        private static final ParseField HIDDEN = new ParseField("hidden");
        private static final ParseField ALLOW_CUSTOM_ROUTING = new ParseField("allow_custom_routing");
        private static final ParseField INDEX_MODE = new ParseField("index_mode");

        public static final ConstructingObjectParser<DataStreamTemplate, Void> PARSER = new ConstructingObjectParser<>(
            "data_stream_template",
            false,
            args -> {
                IndexMode indexMode;
                if (IndexSettings.isTimeSeriesModeEnabled()) {
                    indexMode = args[2] != null ? IndexMode.fromString((String) args[2]) : null;
                } else {
                    indexMode = null;
                }
                return new DataStreamTemplate(args[0] != null && (boolean) args[0], args[1] != null && (boolean) args[1], indexMode);
            }
        );

        static {
            PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), HIDDEN);
            PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ALLOW_CUSTOM_ROUTING);
            if (IndexSettings.isTimeSeriesModeEnabled()) {
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), INDEX_MODE);
            }
        }

        private final boolean hidden;
        private final boolean allowCustomRouting;
        private final IndexMode indexMode;

        public DataStreamTemplate() {
            this(false, false, null);
        }

        public DataStreamTemplate(boolean hidden, boolean allowCustomRouting, IndexMode indexMode) {
            this.hidden = hidden;
            this.allowCustomRouting = allowCustomRouting;
            this.indexMode = indexMode;
        }

        DataStreamTemplate(StreamInput in) throws IOException {
            hidden = in.readBoolean();
            if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
                allowCustomRouting = in.readBoolean();
            } else {
                allowCustomRouting = false;
            }
            if (in.getVersion().onOrAfter(Version.V_8_1_0)) {
                indexMode = in.readOptionalEnum(IndexMode.class);
            } else {
                indexMode = null;
            }
        }

        public static String getTimestampField() {
            return FIXED_TIMESTAMP_FIELD;
        }

        /**
         * A mapping snippet for a backing index with `_data_stream_timestamp` meta field mapper properly configured.
         */
        public static CompressedXContent DATA_STREAM_MAPPING_SNIPPET;

        static {
            try {
                DATA_STREAM_MAPPING_SNIPPET = new CompressedXContent(
                    (builder, params) -> builder.field(
                        MapperService.SINGLE_MAPPING_NAME,
                        Map.of(DataStreamTimestampFieldMapper.NAME, Map.of("enabled", true))
                    )
                );
            } catch (IOException e) {
                throw new AssertionError("no actual IO happens here", e);
            }
        }

        public boolean isHidden() {
            return hidden;
        }

        public boolean isAllowCustomRouting() {
            return allowCustomRouting;
        }

        @Nullable
        public IndexMode getIndexMode() {
            return indexMode;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(hidden);
            if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
                out.writeBoolean(allowCustomRouting);
            }
            if (out.getVersion().onOrAfter(Version.V_8_1_0)) {
                out.writeOptionalEnum(indexMode);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("hidden", hidden);
            builder.field(ALLOW_CUSTOM_ROUTING.getPreferredName(), allowCustomRouting);
            if (indexMode != null) {
                builder.field(INDEX_MODE.getPreferredName(), indexMode);
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DataStreamTemplate that = (DataStreamTemplate) o;
            return hidden == that.hidden && allowCustomRouting == that.allowCustomRouting && indexMode == that.indexMode;
        }

        @Override
        public int hashCode() {
            return Objects.hash(hidden, allowCustomRouting, indexMode);
        }
    }

    public static class Builder {
        private List<String> indexPatterns;
        private Template template;
        private List<String> componentTemplates;
        private Long priority;
        private Long version;
        private Map<String, Object> metadata;
        private DataStreamTemplate dataStreamTemplate;
        private Boolean allowAutoCreate;

        public Builder() {}

        public Builder indexPatterns(List<String> indexPatterns) {
            this.indexPatterns = indexPatterns;
            return this;
        }

        public Builder template(Template template) {
            this.template = template;
            return this;
        }

        public Builder componentTemplates(List<String> componentTemplates) {
            this.componentTemplates = componentTemplates;
            return this;
        }

        public Builder priority(Long priority) {
            this.priority = priority;
            return this;
        }

        public Builder version(Long version) {
            this.version = version;
            return this;
        }

        public Builder metadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder dataStreamTemplate(DataStreamTemplate dataStreamTemplate) {
            this.dataStreamTemplate = dataStreamTemplate;
            return this;
        }

        public Builder allowAutoCreate(Boolean allowAutoCreate) {
            this.allowAutoCreate = allowAutoCreate;
            return this;
        }

        public ComposableIndexTemplate build() {
            return new ComposableIndexTemplate(
                this.indexPatterns,
                this.template,
                this.componentTemplates,
                this.priority,
                this.version,
                this.metadata,
                this.dataStreamTemplate,
                this.allowAutoCreate
            );
        }
    }
}
