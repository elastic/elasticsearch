/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm.dataperiods;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * This class contains the data period configuration for one data stream name pattern. A data period is defined as the minimum time
 * period during which the data (practically a backing index) will have the respective property. Currently, either the least amount of time
 * it's going to be interactive or retained.
 * @param namePattern The name pattern has to be a suffix regex of a data stream name.
 * @param interactivity The minimum amount of time that a backing index is going to be interactive.
 * @param retention The minimum amount of time that a backing index is going to be retained.
 * @param priority The priority of this data period. If there are multiple patterns matching the same data stream name
 *                 the one with the higher priority will be applied.
 */
public record DataPeriod(String namePattern, @Nullable TimeValue interactivity, @Nullable TimeValue retention, int priority)
    implements
        ToXContentObject {

    public static final ParseField NAME_PATTERN_FIELD = new ParseField("name_pattern");
    public static final ParseField INTERACTIVE_FIELD = new ParseField("interactive");
    public static final ParseField RETENTION_FIELD = new ParseField("retention");
    private static final ParseField PRIORITY_FIELD = new ParseField("priority");

    public static final ConstructingObjectParser<DataPeriod, Void> PARSER = new ConstructingObjectParser<>(
        "data_period",
        false,
        (args, unused) -> new DataPeriod((String) args[0], (TimeValue) args[1], (TimeValue) args[2], (int) args[3])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_PATTERN_FIELD);
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.textOrNull(), INTERACTIVE_FIELD.getPreferredName()),
            INTERACTIVE_FIELD,
            ObjectParser.ValueType.STRING_OR_NULL
        );
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.textOrNull(), RETENTION_FIELD.getPreferredName()),
            RETENTION_FIELD,
            ObjectParser.ValueType.STRING_OR_NULL
        );
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), PRIORITY_FIELD);
    }

    public DataPeriod(String namePattern, TimeValue interactivity, TimeValue retention, int priority) {
        if (isSupportedPattern(namePattern) == false) {
            throw new IllegalArgumentException(
                "Name pattern '"
                    + namePattern
                    + "' does not match the allowed pattern styles: \"xxx*\", \"*\" or a concrete data stream name"
            );
        }
        if (interactivity != null && retention != null) {
            if (interactivity.millis() > retention.millis()) {
                throw new IllegalArgumentException(
                    "The interactivity period of your data ["
                        + interactivity.toHumanReadableString(2)
                        + "] cannot be larger than the retention period ["
                        + retention.toHumanReadableString(2)
                        + "]"
                );
            }
        }
        if (priority < 0) {
            throw new IllegalArgumentException("Data period cannot have negative priority.");
        }
        this.namePattern = namePattern;
        this.interactivity = interactivity;
        this.retention = retention;
        this.priority = priority;
    }

    static DataPeriod fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NAME_PATTERN_FIELD.getPreferredName(), namePattern);
        if (interactivity != null) {
            builder.field(INTERACTIVE_FIELD.getPreferredName(), interactivity.getStringRep());
        }
        if (retention != null) {
            builder.field(RETENTION_FIELD.getPreferredName(), retention.getStringRep());
        }
        builder.field(PRIORITY_FIELD.getPreferredName(), priority);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    public boolean match(String name) {
        if (namePattern.equals("*")) {
            return true;
        }
        if (namePattern.contains("*")) {
            return name.startsWith(namePattern.substring(0, namePattern.length() - 1));
        }
        return name.equals(namePattern);
    }

    private boolean isSupportedPattern(String namePattern) {
        return (namePattern.indexOf("*") == namePattern.length() - 1) || namePattern.contains("*") == false;
    }
}
