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

package org.elasticsearch.client.transform.transforms;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class holds the mutable configuration items for a transform
 */
public class TransformConfigUpdate implements ToXContentObject {

    public static final String NAME = "transform_config_update";
    private static final ConstructingObjectParser<TransformConfigUpdate, String> PARSER = new ConstructingObjectParser<>(NAME,
        false,
        (args) -> {
            SourceConfig source = (SourceConfig) args[0];
            DestConfig dest = (DestConfig) args[1];
            TimeValue frequency = args[2] == null ?
                null :
                TimeValue.parseTimeValue((String) args[2], TransformConfig.FREQUENCY.getPreferredName());
            SyncConfig syncConfig = (SyncConfig) args[3];
            String description = (String) args[4];
            return new TransformConfigUpdate(source, dest, frequency, syncConfig, description);
        });

    static {
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> SourceConfig.PARSER.apply(p, null), TransformConfig.SOURCE);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> DestConfig.PARSER.apply(p, null), TransformConfig.DEST);
        PARSER.declareString(optionalConstructorArg(), TransformConfig.FREQUENCY);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> parseSyncConfig(p), TransformConfig.SYNC);
        PARSER.declareString(optionalConstructorArg(), TransformConfig.DESCRIPTION);
    }

    private static SyncConfig parseSyncConfig(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.nextToken(), parser::getTokenLocation);
        SyncConfig syncConfig = parser.namedObject(SyncConfig.class, parser.currentName(), false);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
        return syncConfig;
    }

    private final SourceConfig source;
    private final DestConfig dest;
    private final TimeValue frequency;
    private final SyncConfig syncConfig;
    private final String description;

    public TransformConfigUpdate(final SourceConfig source,
                                 final DestConfig dest,
                                 final TimeValue frequency,
                                 final SyncConfig syncConfig,
                                 final String description) {
        this.source = source;
        this.dest = dest;
        this.frequency = frequency;
        this.syncConfig = syncConfig;
        this.description = description;
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

    @Nullable
    public String getDescription() {
        return description;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        if (source != null) {
            builder.field(TransformConfig.SOURCE.getPreferredName(), source);
        }
        if (dest != null) {
            builder.field(TransformConfig.DEST.getPreferredName(), dest);
        }
        if (frequency != null) {
            builder.field(TransformConfig.FREQUENCY.getPreferredName(), frequency.getStringRep());
        }
        if (syncConfig != null) {
            builder.startObject(TransformConfig.SYNC.getPreferredName());
            builder.field(syncConfig.getName(), syncConfig);
            builder.endObject();
        }
        if (description != null) {
            builder.field(TransformConfig.DESCRIPTION.getPreferredName(), description);
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

        final TransformConfigUpdate that = (TransformConfigUpdate) other;

        return Objects.equals(this.source, that.source)
            && Objects.equals(this.dest, that.dest)
            && Objects.equals(this.frequency, that.frequency)
            && Objects.equals(this.syncConfig, that.syncConfig)
            && Objects.equals(this.description, that.description);
    }

    @Override
    public int hashCode(){
        return Objects.hash(source, dest, frequency, syncConfig, description);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static TransformConfigUpdate fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static class Builder {

        private SourceConfig source;
        private DestConfig dest;
        private TimeValue frequency;
        private SyncConfig syncConfig;
        private String description;

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

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public TransformConfigUpdate build() {
            return new TransformConfigUpdate(source, dest, frequency, syncConfig, description);
        }
    }
}
