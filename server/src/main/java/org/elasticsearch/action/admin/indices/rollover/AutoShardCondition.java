/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.datastreams.autosharding.AutoShardingResult;
import org.elasticsearch.action.datastreams.autosharding.AutoShardingType;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

/**
 * Condition for automatically increasing the number of shards for a data stream. The value is computed when the condition is
 * evaluated.
 */
public class AutoShardCondition extends Condition<String> {
    public static final String NAME = "auto_sharding";

    public static final ParseField AUTO_SHARDING_TYPE = new ParseField("type");
    public static final ParseField CURRENT_NUMBER_OF_SHARDS = new ParseField("current_number_of_shards");
    public static final ParseField TARGET_NUMBER_OF_SHARDS = new ParseField("target_number_of_shards");
    public static final ParseField WRITE_LOAD = new ParseField("write_load");
    private final AutoShardingResult autoShardingResult;

    public AutoShardCondition(AutoShardingResult autoShardingResult) {
        super(NAME, Type.AUTOMATIC);
        if (autoShardingResult.type().equals(AutoShardingType.INCREASE_SHARDS) == false
            && autoShardingResult.type().equals(AutoShardingType.DECREASE_SHARDS) == false) {
            throw new IllegalArgumentException("The autosharding condition only supports the INCREASE and DECREASE shards types");
        }
        this.value = toCSVRep(autoShardingResult);
        this.autoShardingResult = autoShardingResult;
    }

    static String toCSVRep(AutoShardingResult autoShardingResult) {
        return String.join(
            ",",
            List.of(
                AUTO_SHARDING_TYPE.getPreferredName() + "=" + autoShardingResult.type(),
                CURRENT_NUMBER_OF_SHARDS.getPreferredName() + "=" + autoShardingResult.currentNumberOfShards(),
                TARGET_NUMBER_OF_SHARDS.getPreferredName() + "=" + autoShardingResult.targetNumberOfShards(),
                WRITE_LOAD.getPreferredName() + "=" + autoShardingResult.writeLoad()
            )
        );
    }

    // visible for testing
    public static AutoShardingResult fromCSVRep(String csvRep) {
        if (Strings.isNullOrBlank(csvRep)) {
            throw new IllegalArgumentException("The provided csv representation cannot be null");
        }
        String[] fieldsAndValues = csvRep.split(",");
        AutoShardingType type = null;
        Integer currentNumberOfShards = null;
        Integer targetNumberOfShards = null;
        Double writeLoad = null;
        for (String fieldsAndValue : fieldsAndValues) {
            String[] keyValue = fieldsAndValue.split("=");
            if (keyValue.length != 2) {
                throw new IllegalArgumentException("Invalid auto sharding condition representaion: " + csvRep);
            }
            var field = keyValue[0];
            var value = keyValue[1];
            if (AUTO_SHARDING_TYPE.getPreferredName().equals(field)) {
                type = AutoShardingType.valueOf(value);
            } else if (CURRENT_NUMBER_OF_SHARDS.getPreferredName().equals(field)) {
                currentNumberOfShards = Integer.parseInt(value);
            } else if (TARGET_NUMBER_OF_SHARDS.getPreferredName().equals(field)) {
                targetNumberOfShards = Integer.parseInt(value);
            } else if (WRITE_LOAD.getPreferredName().equals(field)) {
                writeLoad = Double.parseDouble(value);
            }
        }
        if (type == null || currentNumberOfShards == null || targetNumberOfShards == null) {
            throw new IllegalArgumentException(
                "Failed to parse auto sharding condition [" + csvRep + "] as not all mandatory fields are present"
            );
        }
        return new AutoShardingResult(type, currentNumberOfShards, targetNumberOfShards, TimeValue.ZERO, writeLoad);
    }

    public AutoShardCondition(StreamInput in) throws IOException {
        super(NAME, Type.AUTOMATIC);
        this.value = in.readString();
        this.autoShardingResult = fromCSVRep(this.value);
    }

    @Override
    public Result evaluate(final Stats stats) {
        return new Result(this, true);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(value);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.field(NAME, value);
    }

    public AutoShardingResult autoShardingResult() {
        return autoShardingResult;
    }

    public static AutoShardCondition fromXContent(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.VALUE_STRING) {
            return new AutoShardCondition(fromCSVRep(parser.text()));
        } else {
            throw new IllegalArgumentException("invalid token when parsing " + NAME + " condition: " + parser.currentToken());
        }
    }

    @Override
    boolean includedInVersion(TransportVersion version) {
        return version.onOrAfter(DataStream.ADDED_AUTO_SHARDING_EVENT_VERSION);
    }
}
