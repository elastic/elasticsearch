/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig.MAX_DESCRIPTION_LENGTH;

/**
 * This class holds the mutable configuration items for a data frame transform
 */
public class DataFrameTransformConfigUpdate implements Writeable, ToXContentObject {

    public static final String NAME = "data_frame_transform_config_update";

    private static final ConstructingObjectParser<DataFrameTransformConfigUpdate, String> PARSER = new ConstructingObjectParser<>(NAME,
        false,
        (args) -> {
            SourceConfig source = (SourceConfig) args[0];
            DestConfig dest = (DestConfig) args[1];
            TimeValue frequency = args[2] == null ?
                null :
                TimeValue.parseTimeValue((String) args[2], DataFrameField.FREQUENCY.getPreferredName());
            SyncConfig syncConfig = (SyncConfig) args[3];
            String description = (String) args[4];
            return new DataFrameTransformConfigUpdate(source, dest, frequency, syncConfig, description);
        });

    static {
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> SourceConfig.fromXContent(p, false), DataFrameField.SOURCE);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> DestConfig.fromXContent(p, false), DataFrameField.DESTINATION);
        PARSER.declareString(optionalConstructorArg(), DataFrameField.FREQUENCY);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> parseSyncConfig(p), DataFrameField.SYNC);
        PARSER.declareString(optionalConstructorArg(), DataFrameField.DESCRIPTION);
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
    private Map<String, String> headers;

    public DataFrameTransformConfigUpdate(final SourceConfig source,
                                          final DestConfig dest,
                                          final TimeValue frequency,
                                          final SyncConfig syncConfig,
                                          final String description){
        this.source = source;
        this.dest = dest;
        this.frequency = frequency;
        this.syncConfig = syncConfig;
        this.description = description;
        if (this.description != null && this.description.length() > MAX_DESCRIPTION_LENGTH) {
            throw new IllegalArgumentException("[description] must be less than 1000 characters in length.");
        }
    }

    public DataFrameTransformConfigUpdate(final StreamInput in) throws IOException {
        source = in.readOptionalWriteable(SourceConfig::new);
        dest = in.readOptionalWriteable(DestConfig::new);
        frequency = in.readOptionalTimeValue();
        description = in.readOptionalString();
        syncConfig = in.readOptionalNamedWriteable(SyncConfig.class);
        if (in.readBoolean()) {
            setHeaders(in.readMap(StreamInput::readString, StreamInput::readString));
        }
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

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeOptionalWriteable(source);
        out.writeOptionalWriteable(dest);
        out.writeOptionalTimeValue(frequency);
        out.writeOptionalString(description);
        out.writeOptionalNamedWriteable(syncConfig);
        if (headers != null) {
            out.writeBoolean(true);
            out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        if (source != null) {
            builder.field(DataFrameField.SOURCE.getPreferredName(), source);
        }
        if (dest != null) {
            builder.field(DataFrameField.DESTINATION.getPreferredName(), dest);
        }
        if (frequency != null) {
            builder.field(DataFrameField.FREQUENCY.getPreferredName(), frequency.getStringRep());
        }
        if (syncConfig != null) {
            builder.startObject(DataFrameField.SYNC.getPreferredName());
            builder.field(syncConfig.getWriteableName(), syncConfig);
            builder.endObject();
        }
        if (description != null) {
            builder.field(DataFrameField.DESCRIPTION.getPreferredName(), description);
        }
        if (headers != null) {
            builder.field(DataFrameTransformConfig.HEADERS.getPreferredName(), headers);
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

        final DataFrameTransformConfigUpdate that = (DataFrameTransformConfigUpdate) other;

        return Objects.equals(this.source, that.source)
                && Objects.equals(this.dest, that.dest)
                && Objects.equals(this.frequency, that.frequency)
                && Objects.equals(this.syncConfig, that.syncConfig)
                && Objects.equals(this.description, that.description)
                && Objects.equals(this.headers, that.headers);
    }

    @Override
    public int hashCode(){
        return Objects.hash(source, dest, frequency, syncConfig, description, headers);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public static DataFrameTransformConfigUpdate fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public boolean isNoop(DataFrameTransformConfig config) {
        return isNullOrEqual(source, config.getSource())
            && isNullOrEqual(dest, config.getDestination())
            && isNullOrEqual(frequency, config.getFrequency())
            && isNullOrEqual(syncConfig, config.getSyncConfig())
            && isNullOrEqual(description, config.getDescription())
            && isNullOrEqual(headers, config.getHeaders());
    }

    private boolean isNullOrEqual(Object lft, Object rgt) {
        return lft == null || lft.equals(rgt);
    }

    public DataFrameTransformConfig apply(DataFrameTransformConfig config) {
        if (isNoop(config)) {
            return config;
        }
        DataFrameTransformConfig.Builder builder = new DataFrameTransformConfig.Builder(config);
        if (source != null) {
            builder.setSource(source);
        }
        if (dest != null) {
            builder.setDest(dest);
        }
        if (frequency != null) {
            builder.setFrequency(frequency);
        }
        if (syncConfig != null) {
            String currentConfigName = config.getSyncConfig() == null ? "null" : config.getSyncConfig().getWriteableName();
            if (syncConfig.getWriteableName().equals(currentConfigName) == false) {
                throw new ElasticsearchStatusException(
                    DataFrameMessages.getMessage(DataFrameMessages.DATA_FRAME_UPDATE_CANNOT_CHANGE_SYNC_METHOD,
                        config.getId(),
                        currentConfigName,
                        syncConfig.getWriteableName()),
                    RestStatus.BAD_REQUEST);
            }
            builder.setSyncConfig(syncConfig);
        }
        if (description != null) {
            builder.setDescription(description);
        }
        if (headers != null) {
            builder.setHeaders(headers);
        }
        builder.setVersion(Version.CURRENT);
        return builder.build();
    }
}
