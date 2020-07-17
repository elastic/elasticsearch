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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;

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
public class ComposableIndexTemplate extends AbstractDiffable<ComposableIndexTemplate> implements ToXContentObject {
    private static final ParseField INDEX_PATTERNS = new ParseField("index_patterns");
    private static final ParseField TEMPLATE = new ParseField("template");
    private static final ParseField PRIORITY = new ParseField("priority");
    private static final ParseField COMPOSED_OF = new ParseField("composed_of");
    private static final ParseField VERSION = new ParseField("version");
    private static final ParseField METADATA = new ParseField("_meta");
    private static final ParseField DATA_STREAM = new ParseField("data_stream");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<ComposableIndexTemplate, Void> PARSER = new ConstructingObjectParser<>("index_template",
        false,
        a -> new ComposableIndexTemplate((List<String>) a[0],
            (Template) a[1],
            (List<String>) a[2],
            (Long) a[3],
            (Long) a[4],
            (Map<String, Object>) a[5],
            (DataStreamTemplate) a[6]));

    static {
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), INDEX_PATTERNS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), Template.PARSER, TEMPLATE);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), COMPOSED_OF);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), PRIORITY);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), VERSION);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), METADATA);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), DataStreamTemplate.PARSER, DATA_STREAM);
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

    static Diff<ComposableIndexTemplate> readITV2DiffFrom(StreamInput in) throws IOException {
        return AbstractDiffable.readDiffFrom(ComposableIndexTemplate::new, in);
    }

    public static ComposableIndexTemplate parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public ComposableIndexTemplate(List<String> indexPatterns, @Nullable Template template, @Nullable List<String> componentTemplates,
                                   @Nullable Long priority, @Nullable Long version, @Nullable Map<String, Object> metadata) {
        this(indexPatterns, template, componentTemplates, priority, version, metadata, null);
    }

    public ComposableIndexTemplate(List<String> indexPatterns, @Nullable Template template, @Nullable List<String> componentTemplates,
                                   @Nullable Long priority, @Nullable Long version, @Nullable Map<String, Object> metadata,
                                   @Nullable DataStreamTemplate dataStreamTemplate) {
        this.indexPatterns = indexPatterns;
        this.template = template;
        this.componentTemplates = componentTemplates;
        this.priority = priority;
        this.version = version;
        this.metadata = metadata;
        this.dataStreamTemplate = dataStreamTemplate;
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
        if (in.getVersion().onOrAfter(Version.V_7_9_0)) {
            this.dataStreamTemplate = in.readOptionalWriteable(DataStreamTemplate::new);
        } else {
            this.dataStreamTemplate = null;
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

    public Long priority() {
        return priority;
    }

    public long priorityOrZero() {
        if (priority == null) {
            return 0L;
        }
        return priority;
    }

    public Long version() {
        return version;
    }

    public Map<String, Object> metadata() {
        return metadata;
    }

    public DataStreamTemplate getDataStreamTemplate() {
        return dataStreamTemplate;
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
        out.writeMap(this.metadata);
        if (out.getVersion().onOrAfter(Version.V_7_9_0)) {
            out.writeOptionalWriteable(dataStreamTemplate);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX_PATTERNS.getPreferredName(), this.indexPatterns);
        if (this.template != null) {
            builder.field(TEMPLATE.getPreferredName(), this.template);
        }
        if (this.componentTemplates != null) {
            builder.field(COMPOSED_OF.getPreferredName(), this.componentTemplates);
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
            builder.field(DATA_STREAM.getPreferredName(), dataStreamTemplate);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.indexPatterns, this.template, this.componentTemplates, this.priority, this.version,
            this.metadata, this.dataStreamTemplate);
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
        return Objects.equals(this.indexPatterns, other.indexPatterns) &&
            Objects.equals(this.template, other.template) &&
            Objects.equals(this.componentTemplates, other.componentTemplates) &&
            Objects.equals(this.priority, other.priority) &&
            Objects.equals(this.version, other.version) &&
            Objects.equals(this.metadata, other.metadata) &&
            Objects.equals(this.dataStreamTemplate, other.dataStreamTemplate);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class DataStreamTemplate implements Writeable, ToXContentObject {

        private static final ObjectParser<DataStreamTemplate, Void> PARSER = new ObjectParser<>(
            "data_stream_template",
            DataStreamTemplate::new
        );

        public DataStreamTemplate() {
        }

        public String getTimestampField() {
            return FIXED_TIMESTAMP_FIELD;
        }

        DataStreamTemplate(StreamInput in) {
            this();
        }

        /**
         * @return a mapping snippet for a backing index with `_data_stream_timestamp` meta field mapper properly configured.
         */
        public Map<String, Object> getDataStreamMappingSnippet() {
            // _data_stream_timestamp meta fields default to @timestamp:
            return Map.of(MapperService.SINGLE_MAPPING_NAME, Map.of("_data_stream_timestamp", Map.of("enabled", true)));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            return o != null && getClass() == o.getClass();
        }

        @Override
        public int hashCode() {
            return DataStreamTemplate.class.hashCode();
        }
    }
}
