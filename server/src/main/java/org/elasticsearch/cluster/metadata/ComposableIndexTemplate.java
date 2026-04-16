/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.metadata.Template.NamedTemplateDecorator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * An index template consists of a set of index patterns, an optional template, and a list of
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
    private static final ParseField IGNORE_MISSING_COMPONENT_TEMPLATES = new ParseField("ignore_missing_component_templates");
    private static final ParseField DEPRECATED = new ParseField("deprecated");
    private static final ParseField CREATED_DATE_MILLIS = new ParseField("created_date_millis");
    private static final ParseField CREATED_DATE = new ParseField("created_date");
    private static final ParseField MODIFIED_DATE_MILLIS = new ParseField("modified_date_millis");
    private static final ParseField MODIFIED_DATE = new ParseField("modified_date");
    public static final CompressedXContent EMPTY_MAPPINGS;
    static {
        try {
            EMPTY_MAPPINGS = new CompressedXContent(Map.of());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ComposableIndexTemplate, NamedTemplateDecorator> PARSER = new ConstructingObjectParser<>(
        "index_template",
        false,
        a -> ComposableIndexTemplate.builder()
            .indexPatterns((List<String>) a[0])
            .template((Template) a[1])
            .componentTemplates((List<String>) a[2])
            .priority((Long) a[3])
            .version((Long) a[4])
            .metadata((Map<String, Object>) a[5])
            .dataStreamTemplate((DataStreamTemplate) a[6])
            .allowAutoCreate((Boolean) a[7])
            .ignoreMissingComponentTemplates((List<String>) a[8])
            .deprecated((Boolean) a[9])
            .createdDate((Long) a[10])
            .modifiedDate((Long) a[11])
            .build()
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
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), IGNORE_MISSING_COMPONENT_TEMPLATES);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), DEPRECATED);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), CREATED_DATE_MILLIS);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), MODIFIED_DATE_MILLIS);
    }

    private static final TransportVersion INDEX_TEMPLATE_TRACKING_INFO = TransportVersion.fromName("index_template_tracking_info");

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
    @Nullable
    private final List<String> ignoreMissingComponentTemplates;
    @Nullable
    private final Boolean deprecated;
    @Nullable
    private final Long createdDateMillis;
    @Nullable
    private final Long modifiedDateMillis;

    static Diff<ComposableIndexTemplate> readITV2DiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(ComposableIndexTemplate::new, in);
    }

    public static ComposableIndexTemplate parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, NamedTemplateDecorator.DEFAULT);
    }

    public static ComposableIndexTemplate parse(XContentParser parser, String templateName, Template.TemplateDecorator decorator) {
        return PARSER.apply(parser, new NamedTemplateDecorator(templateName, decorator));
    }

    public static Builder builder() {
        return new Builder();
    }

    private ComposableIndexTemplate(final Builder b) {
        this.indexPatterns = b.indexPatterns;
        this.template = b.template;
        this.componentTemplates = b.componentTemplates == null ? List.of() : b.componentTemplates;
        this.priority = b.priority;
        this.version = b.version;
        this.metadata = b.metadata;
        this.dataStreamTemplate = b.dataStreamTemplate;
        this.allowAutoCreate = b.allowAutoCreate;
        this.ignoreMissingComponentTemplates = b.ignoreMissingComponentTemplates;
        this.deprecated = b.deprecated;
        this.createdDateMillis = b.createdDateMillis;
        this.modifiedDateMillis = b.modifiedDateMillis;
    }

    public ComposableIndexTemplate(StreamInput in) throws IOException {
        this.indexPatterns = in.readStringCollectionAsList();
        if (in.readBoolean()) {
            this.template = new Template(in);
        } else {
            this.template = null;
        }
        this.componentTemplates = in.readOptionalStringCollectionAsList();
        this.priority = in.readOptionalVLong();
        this.version = in.readOptionalVLong();
        this.metadata = in.readGenericMap();
        this.dataStreamTemplate = in.readOptionalWriteable(DataStreamTemplate::new);
        this.allowAutoCreate = in.readOptionalBoolean();
        this.ignoreMissingComponentTemplates = in.readOptionalStringCollectionAsList();
        this.deprecated = in.readOptionalBoolean();
        if (in.getTransportVersion().supports(INDEX_TEMPLATE_TRACKING_INFO)) {
            this.createdDateMillis = in.readOptionalLong();
            this.modifiedDateMillis = in.readOptionalLong();
        } else {
            this.createdDateMillis = null;
            this.modifiedDateMillis = null;
        }
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

    /**
     * Returns the <b>required</b> component templates, i.e. such that are not allowed to be missing, as in
     * {@link #ignoreMissingComponentTemplates}.
     * @return a list of required component templates
     */
    public List<String> getRequiredComponentTemplates() {
        if (componentTemplates == null) {
            return List.of();
        }
        if (ignoreMissingComponentTemplates == null) {
            return componentTemplates;
        }
        // note: this loop is unrolled rather than streaming-style because it's hot enough to show up in a flamegraph
        List<String> required = new ArrayList<>(componentTemplates.size());
        for (String template : componentTemplates) {
            if (ignoreMissingComponentTemplates.contains(template) == false) {
                required.add(template);
            }
        }
        return Collections.unmodifiableList(required);
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

    @Nullable
    public List<String> getIgnoreMissingComponentTemplates() {
        return ignoreMissingComponentTemplates;
    }

    public boolean isDeprecated() {
        return Boolean.TRUE.equals(deprecated);
    }

    public Optional<Long> createdDateMillis() {
        return Optional.ofNullable(createdDateMillis);
    }

    public Optional<Long> modifiedDateMillis() {
        return Optional.ofNullable(modifiedDateMillis);
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
        out.writeOptionalStringCollection(ignoreMissingComponentTemplates);
        out.writeOptionalBoolean(deprecated);
        if (out.getTransportVersion().supports(INDEX_TEMPLATE_TRACKING_INFO)) {
            out.writeOptionalLong(createdDateMillis);
            out.writeOptionalLong(modifiedDateMillis);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, params, null);
    }

    /**
     * Converts the composable index template to XContent and passes the RolloverConditions, when provided, to the template.
     */
    public XContentBuilder toXContent(XContentBuilder builder, Params params, @Nullable RolloverConfiguration rolloverConfiguration)
        throws IOException {
        builder.startObject();
        builder.stringListField(INDEX_PATTERNS.getPreferredName(), this.indexPatterns);
        if (this.template != null) {
            builder.field(TEMPLATE.getPreferredName());
            this.template.toXContent(builder, params, rolloverConfiguration);
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
        if (this.ignoreMissingComponentTemplates != null) {
            builder.stringListField(IGNORE_MISSING_COMPONENT_TEMPLATES.getPreferredName(), ignoreMissingComponentTemplates);
        }
        if (this.deprecated != null) {
            builder.field(DEPRECATED.getPreferredName(), deprecated);
        }
        if (this.createdDateMillis != null) {
            builder.timestampFieldsFromUnixEpochMillis(
                CREATED_DATE_MILLIS.getPreferredName(),
                CREATED_DATE.getPreferredName(),
                this.createdDateMillis
            );
        }
        if (this.modifiedDateMillis != null) {
            builder.timestampFieldsFromUnixEpochMillis(
                MODIFIED_DATE_MILLIS.getPreferredName(),
                MODIFIED_DATE.getPreferredName(),
                this.modifiedDateMillis
            );
        }
        builder.endObject();
        return builder;
    }

    /*
     * Merges the given settings into the settings in this ComposableIndexTemplate. Any null values in the
     * given settings are removed from the settings in the returned ComposableIndexTemplate. If this
     * ComposableIndexTemplate has no settings, the given settings are the only ones in the returned template
     * (with any null values removed). If this ComposableIndexTemplate has no template, an empty template with
     * those settings is created. If the given settings are empty, this ComposableIndexTemplate is just
     * returned unchanged. This method never changes this object.
     */
    public ComposableIndexTemplate mergeSettings(Settings settings) {
        Objects.requireNonNull(settings);
        if (Settings.EMPTY.equals(settings)) {
            return this;
        }
        ComposableIndexTemplate.Builder mergedIndexTemplateBuilder = this.toBuilder();
        Template.Builder mergedTemplateBuilder;
        Settings templateSettings;
        if (this.template() == null) {
            mergedTemplateBuilder = Template.builder();
            templateSettings = null;
        } else {
            mergedTemplateBuilder = Template.builder(this.template());
            templateSettings = this.template().settings();
        }
        mergedTemplateBuilder.settings(templateSettings == null ? settings : templateSettings.merge(settings));
        mergedIndexTemplateBuilder.template(mergedTemplateBuilder);
        return mergedIndexTemplateBuilder.build();
    }

    public ComposableIndexTemplate mergeMappings(CompressedXContent mappings) throws IOException {
        Objects.requireNonNull(mappings);
        if (Mapping.EMPTY.toCompressedXContent().equals(mappings) && this.template() != null && this.template().mappings() != null) {
            return this;
        }
        ComposableIndexTemplate.Builder mergedIndexTemplateBuilder = this.toBuilder();
        Template.Builder mergedTemplateBuilder;
        CompressedXContent templateMappings;
        if (this.template() == null) {
            mergedTemplateBuilder = Template.builder();
            templateMappings = null;
        } else {
            mergedTemplateBuilder = Template.builder(this.template());
            templateMappings = this.template().mappings();
        }
        mergedTemplateBuilder.mappings(templateMappings == null ? mappings : merge(templateMappings, mappings));
        mergedIndexTemplateBuilder.template(mergedTemplateBuilder);
        return mergedIndexTemplateBuilder.build();
    }

    @SuppressWarnings("unchecked")
    public static CompressedXContent merge(CompressedXContent originalMapping, CompressedXContent mappingAddition) throws IOException {
        Map<String, Object> mappingAdditionMap = XContentHelper.convertToMap(mappingAddition.uncompressed(), true, XContentType.JSON).v2();
        Map<String, Object> combinedMappingMap = new HashMap<>();
        if (originalMapping != null) {
            Map<String, Object> originalMappingMap = XContentHelper.convertToMap(originalMapping.uncompressed(), true, XContentType.JSON)
                .v2();
            if (originalMappingMap.containsKey(MapperService.SINGLE_MAPPING_NAME)) {
                combinedMappingMap.putAll((Map<String, ?>) originalMappingMap.get(MapperService.SINGLE_MAPPING_NAME));
            } else {
                combinedMappingMap.putAll(originalMappingMap);
            }
        }
        XContentHelper.update(combinedMappingMap, mappingAdditionMap, true);
        return convertMappingMapToXContent(combinedMappingMap);
    }

    public static CompressedXContent convertMappingMapToXContent(Map<String, ?> rawAdditionalMapping) throws IOException {
        CompressedXContent compressedXContent;
        if (rawAdditionalMapping.isEmpty()) {
            compressedXContent = EMPTY_MAPPINGS;
        } else {
            try (var parser = XContentHelper.mapToXContentParser(XContentParserConfiguration.EMPTY, rawAdditionalMapping)) {
                compressedXContent = mappingFromXContent(parser);
            }
        }
        return compressedXContent;
    }

    private static CompressedXContent mappingFromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        if (token == XContentParser.Token.START_OBJECT) {
            return new CompressedXContent(Strings.toString(XContentFactory.jsonBuilder().map(parser.mapOrdered())));
        } else {
            throw new IllegalArgumentException("Unexpected token: " + token);
        }
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
            this.allowAutoCreate,
            this.ignoreMissingComponentTemplates,
            this.deprecated,
            this.createdDateMillis,
            this.modifiedDateMillis
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
            && Objects.equals(this.allowAutoCreate, other.allowAutoCreate)
            && Objects.equals(this.ignoreMissingComponentTemplates, other.ignoreMissingComponentTemplates)
            && Objects.equals(deprecated, other.deprecated)
            && Objects.equals(createdDateMillis, other.createdDateMillis)
            && Objects.equals(modifiedDateMillis, other.modifiedDateMillis);
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

    public Builder toBuilder() {
        return new Builder(this);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class DataStreamTemplate implements Writeable, ToXContentObject {

        private static final ParseField HIDDEN = new ParseField("hidden");
        private static final ParseField ALLOW_CUSTOM_ROUTING = new ParseField("allow_custom_routing");
        private static final ParseField FAILURE_STORE = new ParseField("failure_store");

        static final ConstructingObjectParser<DataStreamTemplate, NamedTemplateDecorator> PARSER = new ConstructingObjectParser<>(
            "data_stream_template",
            false,
            args -> new DataStreamTemplate(args[0] != null && (boolean) args[0], args[1] != null && (boolean) args[1])
        );

        static {
            PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), HIDDEN);
            PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ALLOW_CUSTOM_ROUTING);
            PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), FAILURE_STORE);
        }

        private final boolean hidden;
        private final boolean allowCustomRouting;

        public DataStreamTemplate() {
            this(false, false);
        }

        public DataStreamTemplate(boolean hidden, boolean allowCustomRouting) {
            this.hidden = hidden;
            this.allowCustomRouting = allowCustomRouting;
        }

        DataStreamTemplate(StreamInput in) throws IOException {
            hidden = in.readBoolean();
            allowCustomRouting = in.readBoolean();
        }

        /**
         * A mapping snippet for a backing index with `_data_stream_timestamp` meta field mapper properly configured.
         */
        public static final CompressedXContent DATA_STREAM_MAPPING_SNIPPET;

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

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(hidden);
            out.writeBoolean(allowCustomRouting);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("hidden", hidden);
            builder.field(ALLOW_CUSTOM_ROUTING.getPreferredName(), allowCustomRouting);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DataStreamTemplate that = (DataStreamTemplate) o;
            return hidden == that.hidden && allowCustomRouting == that.allowCustomRouting;
        }

        @Override
        public int hashCode() {
            return Objects.hash(hidden, allowCustomRouting);
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
        private List<String> ignoreMissingComponentTemplates;
        private Boolean deprecated;
        private Long createdDateMillis;
        private Long modifiedDateMillis;

        /**
         * @deprecated use {@link ComposableIndexTemplate#builder()}
         */
        @Deprecated(forRemoval = true)
        public Builder() {}

        private Builder(ComposableIndexTemplate template) {
            this.indexPatterns = template.indexPatterns;
            this.template = template.template;
            this.componentTemplates = template.componentTemplates;
            this.priority = template.priority;
            this.version = template.version;
            this.metadata = template.metadata;
            this.dataStreamTemplate = template.dataStreamTemplate;
            this.allowAutoCreate = template.allowAutoCreate;
            this.ignoreMissingComponentTemplates = template.ignoreMissingComponentTemplates;
            this.deprecated = template.deprecated;
            this.createdDateMillis = template.createdDateMillis;
            this.modifiedDateMillis = template.modifiedDateMillis;
        }

        public Builder indexPatterns(List<String> indexPatterns) {
            this.indexPatterns = indexPatterns;
            return this;
        }

        public Builder template(Template template) {
            this.template = template;
            return this;
        }

        public Builder template(Template.Builder template) {
            this.template = template.build();
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

        public Builder ignoreMissingComponentTemplates(List<String> ignoreMissingComponentTemplates) {
            this.ignoreMissingComponentTemplates = ignoreMissingComponentTemplates;
            return this;
        }

        public Builder deprecated(@Nullable Boolean deprecated) {
            this.deprecated = deprecated;
            return this;
        }

        public Builder createdDate(@Nullable Long createdDate) {
            this.createdDateMillis = createdDate;
            return this;
        }

        public Builder modifiedDate(@Nullable Long modifiedDate) {
            this.modifiedDateMillis = modifiedDate;
            return this;
        }

        public ComposableIndexTemplate build() {
            return new ComposableIndexTemplate(this);
        }
    }
}
