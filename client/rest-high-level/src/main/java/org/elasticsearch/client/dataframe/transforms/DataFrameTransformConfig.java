/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.dataframe.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.client.dataframe.transforms.pivot.PivotConfig;
import org.elasticsearch.client.dataframe.transforms.util.TimeUtil;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class DataFrameTransformConfig implements ToXContentObject {

    public static final ParseField ID = new ParseField("id");
    public static final ParseField SOURCE = new ParseField("source");
    public static final ParseField DEST = new ParseField("dest");
    public static final ParseField FREQUENCY = new ParseField("frequency");
    public static final ParseField DESCRIPTION = new ParseField("description");
    public static final ParseField SYNC = new ParseField("sync");
    public static final ParseField VERSION = new ParseField("version");
    public static final ParseField CREATE_TIME = new ParseField("create_time");
    // types of transforms
    public static final ParseField PIVOT_TRANSFORM = new ParseField("pivot");

    private final String id;
    private final SourceConfig source;
    private final DestConfig dest;
    private final TimeValue frequency;
    private final SyncConfig syncConfig;
    private final PivotConfig pivotConfig;
    private final String description;
    private final Version transformVersion;
    private final Instant createTime;

    public static final ConstructingObjectParser<DataFrameTransformConfig, Void> PARSER =
            new ConstructingObjectParser<>("data_frame_transform", true,
                (args) -> {
                    String id = (String) args[0];
                    SourceConfig source = (SourceConfig) args[1];
                    DestConfig dest = (DestConfig) args[2];
                    TimeValue frequency = (TimeValue) args[3];
                    SyncConfig syncConfig = (SyncConfig) args[4];
                    PivotConfig pivotConfig = (PivotConfig) args[5];
                    String description = (String)args[6];
                    Instant createTime = (Instant)args[7];
                    String transformVersion = (String)args[8];
                    return new DataFrameTransformConfig(id,
                        source,
                        dest,
                        frequency,
                        syncConfig,
                        pivotConfig,
                        description,
                        createTime,
                        transformVersion);
                });

    static {
        PARSER.declareString(constructorArg(), ID);
        PARSER.declareObject(constructorArg(), (p, c) -> SourceConfig.PARSER.apply(p, null), SOURCE);
        PARSER.declareObject(constructorArg(), (p, c) -> DestConfig.PARSER.apply(p, null), DEST);
        PARSER.declareField(optionalConstructorArg(), p -> TimeValue.parseTimeValue(p.text(), FREQUENCY.getPreferredName()),
            FREQUENCY, ObjectParser.ValueType.STRING);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> parseSyncConfig(p), SYNC);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> PivotConfig.fromXContent(p), PIVOT_TRANSFORM);
        PARSER.declareString(optionalConstructorArg(), DESCRIPTION);
        PARSER.declareField(optionalConstructorArg(),
            p -> TimeUtil.parseTimeFieldToInstant(p, CREATE_TIME.getPreferredName()), CREATE_TIME, ObjectParser.ValueType.VALUE);
        PARSER.declareString(optionalConstructorArg(), VERSION);
    }

    private static SyncConfig parseSyncConfig(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
        SyncConfig syncConfig = parser.namedObject(SyncConfig.class, parser.currentName(), true);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
        return syncConfig;
    }


    public static DataFrameTransformConfig fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    /**
     * Helper method for previewing a data frame transform configuration
     *
     * The DataFrameTransformConfig returned from this method should only be used for previewing the resulting data.
     *
     * A new, valid, DataFrameTransformConfig with an appropriate destination and ID will have to be constructed to create
     * the transform.
     * @param source Source configuration for gathering the data
     * @param pivotConfig Pivot config to preview
     * @return A DataFrameTransformConfig to preview, NOTE it will have a {@code null} id, destination and index.
     */
    public static DataFrameTransformConfig forPreview(final SourceConfig source, final PivotConfig pivotConfig) {
        return new DataFrameTransformConfig(null, source, null, null, null, pivotConfig, null, null, null);
    }

    DataFrameTransformConfig(final String id,
                             final SourceConfig source,
                             final DestConfig dest,
                             final TimeValue frequency,
                             final SyncConfig syncConfig,
                             final PivotConfig pivotConfig,
                             final String description,
                             final Instant createTime,
                             final String version) {
        this.id = id;
        this.source = source;
        this.dest = dest;
        this.frequency = frequency;
        this.syncConfig = syncConfig;
        this.pivotConfig = pivotConfig;
        this.description = description;
        this.createTime = createTime == null ? null : Instant.ofEpochMilli(createTime.toEpochMilli());
        this.transformVersion = version == null ? null : Version.fromString(version);
    }

    public String getId() {
        return id;
    }

    public SourceConfig getSource() {
        return source;
    }

    public DestConfig getDestination() {
        return dest;
    }

    public TimeValue getFrequency() {
        return frequency;
    }

    public SyncConfig getSyncConfig() {
        return syncConfig;
    }

    public PivotConfig getPivotConfig() {
        return pivotConfig;
    }

    public Version getVersion() {
        return transformVersion;
    }

    public Instant getCreateTime() {
        return createTime;
    }

    @Nullable
    public String getDescription() {
        return description;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        if (id != null) {
            builder.field(ID.getPreferredName(), id);
        }
        if (source != null) {
            builder.field(SOURCE.getPreferredName(), source);
        }
        if (dest != null) {
            builder.field(DEST.getPreferredName(), dest);
        }
        if (frequency != null) {
            builder.field(FREQUENCY.getPreferredName(), frequency.getStringRep());
        }
        if (syncConfig != null) {
            builder.startObject(SYNC.getPreferredName());
            builder.field(syncConfig.getName(), syncConfig);
            builder.endObject();
        }
        if (pivotConfig != null) {
            builder.field(PIVOT_TRANSFORM.getPreferredName(), pivotConfig);
        }
        if (description != null) {
            builder.field(DESCRIPTION.getPreferredName(), description);
        }
        if (createTime != null) {
            builder.timeField(CREATE_TIME.getPreferredName(), CREATE_TIME.getPreferredName() + "_string", createTime.toEpochMilli());
        }
        if (transformVersion != null) {
            builder.field(VERSION.getPreferredName(), transformVersion);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final DataFrameTransformConfig that = (DataFrameTransformConfig) other;

        return Objects.equals(this.id, that.id)
            && Objects.equals(this.source, that.source)
            && Objects.equals(this.dest, that.dest)
            && Objects.equals(this.frequency, that.frequency)
            && Objects.equals(this.description, that.description)
            && Objects.equals(this.syncConfig, that.syncConfig)
            && Objects.equals(this.transformVersion, that.transformVersion)
            && Objects.equals(this.createTime, that.createTime)
            && Objects.equals(this.pivotConfig, that.pivotConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, source, dest, frequency, syncConfig, pivotConfig, description);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private String id;
        private SourceConfig source;
        private DestConfig dest;
        private TimeValue frequency;
        private SyncConfig syncConfig;
        private PivotConfig pivotConfig;
        private String description;

        public Builder setId(String id) {
            this.id = id;
            return this;
        }

        public Builder setSource(SourceConfig source) {
            this.source = source;
            return this;
        }

        public Builder setDest(DestConfig dest) {
            this.dest = dest;
            return this;
        }

        public Builder setFrequency(TimeValue frequency) {
            this.frequency = frequency;
            return this;
        }

        public Builder setSyncConfig(SyncConfig syncConfig) {
            this.syncConfig = syncConfig;
            return this;
        }

        public Builder setPivotConfig(PivotConfig pivotConfig) {
            this.pivotConfig = pivotConfig;
            return this;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public DataFrameTransformConfig build() {
            return new DataFrameTransformConfig(id, source, dest, frequency, syncConfig, pivotConfig, description, null, null);
        }
    }
}
