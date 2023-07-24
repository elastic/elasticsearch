/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * This class holds the configuration of the rollover conditions as they are defined in data stream lifecycle. Currently, it can handle
 * automatic configuration for the max index age condition.
 */
public class RolloverConfiguration implements Writeable, ToXContentObject {

    // The conditions that have concrete values
    private final RolloverConditions concreteConditions;
    // The conditions that have the value `auto`, currently only max_age is supported
    private final Set<String> automaticConditions;

    public RolloverConfiguration(RolloverConditions concreteConditions, Set<String> automaticConditions) {
        validate(concreteConditions, automaticConditions);
        this.concreteConditions = concreteConditions;
        this.automaticConditions = automaticConditions;
    }

    private static void validate(RolloverConditions concreteConditions, Set<String> automaticConditions) {
        if (automaticConditions.isEmpty()) {
            return;
        }
        // The only supported condition is max_age
        if (automaticConditions.size() > 1 || automaticConditions.contains(MaxAgeCondition.NAME) == false) {
            throw new IllegalArgumentException(
                "Invalid automatic configuration for " + automaticConditions + ", only condition 'max_age' is supported."
            );
        }
        // If max_age is automatic there should be no concrete value provided
        if (concreteConditions.getMaxAge() != null) {
            throw new IllegalArgumentException(
                "Invalid configuration for 'max_age' can be either have a value or be automatic but not both."
            );
        }
    }

    public RolloverConfiguration(RolloverConditions concreteConditions) {
        this(concreteConditions, Set.of());
    }

    public RolloverConfiguration(StreamInput in) throws IOException {
        this(new RolloverConditions(in), in.readSet(StreamInput::readString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeWriteable(concreteConditions);
        out.writeCollection(automaticConditions, StreamOutput::writeString);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        concreteConditions.toXContentFragment(builder, params);
        for (String automaticCondition : automaticConditions) {
            builder.field(automaticCondition, "auto");
        }
        builder.endObject();
        return builder;
    }

    /**
     * Evaluates the automatic conditions and converts the whole configuration to XContent.
     * For the automatic conditions is also adds the suffix [automatic]
     */
    public XContentBuilder evaluateAndConvertToXContent(XContentBuilder builder, Params params, TimeValue retention) throws IOException {
        builder.startObject();
        concreteConditions.toXContentFragment(builder, params);
        for (String automaticCondition : automaticConditions) {
            builder.field(automaticCondition, evaluateMaxAgeCondition(retention) + " [automatic]");
        }
        builder.endObject();
        return builder;
    }

    /**
     * Evaluates all the automatic conditions based on the provided arguments.
     */
    public RolloverConditions resolveRolloverConditions(@Nullable TimeValue dataRetention) {
        if (automaticConditions.isEmpty()) {
            return concreteConditions;
        }
        RolloverConditions.Builder builder = RolloverConditions.newBuilder(concreteConditions);
        if (automaticConditions.contains(MaxAgeCondition.NAME)) {
            builder.addMaxIndexAgeCondition(evaluateMaxAgeCondition(dataRetention));
        }
        return builder.build();
    }

    // Visible for testing
    public RolloverConditions getConcreteConditions() {
        return concreteConditions;
    }

    // Visible for testing
    public Set<String> getAutomaticConditions() {
        return automaticConditions;
    }

    /**
     * Parses a cluster setting configuration, it expects it to have the following format: "condition1=value1,condition2=value2"
     * @throws SettingsException if the input is invalid, if there are unknown conditions or invalid format values.
     * @throws IllegalArgumentException if the input is null or blank.
     */
    public static RolloverConfiguration parseSetting(String input, String setting) {
        if (Strings.isNullOrBlank(input)) {
            throw new IllegalArgumentException("The rollover conditions cannot be null or blank");
        }
        String[] sConditions = input.split(",");
        RolloverConfiguration.ValueParser valueParser = new RolloverConfiguration.ValueParser();
        for (String sCondition : sConditions) {
            String[] keyValue = sCondition.split("=");
            if (keyValue.length != 2) {
                throw new SettingsException("Invalid condition: '{}', format must be 'condition=value'", sCondition);
            }
            var condition = keyValue[0];
            var value = keyValue[1];
            if (MaxSizeCondition.NAME.equals(condition)) {
                valueParser.addMaxIndexSizeCondition(value, setting);
            } else if (MaxPrimaryShardSizeCondition.NAME.equals(condition)) {
                valueParser.addMaxPrimaryShardSizeCondition(value, setting);
            } else if (MaxAgeCondition.NAME.equals(condition)) {
                valueParser.addMaxIndexAgeCondition(value, setting);
            } else if (MaxDocsCondition.NAME.equals(condition)) {
                valueParser.addMaxIndexDocsCondition(value, setting);
            } else if (MaxPrimaryShardDocsCondition.NAME.equals(condition)) {
                valueParser.addMaxPrimaryShardDocsCondition(value, setting);
            } else if (MinSizeCondition.NAME.equals(condition)) {
                valueParser.addMinIndexSizeCondition(value, setting);
            } else if (MinPrimaryShardSizeCondition.NAME.equals(condition)) {
                valueParser.addMinPrimaryShardSizeCondition(value, setting);
            } else if (MinAgeCondition.NAME.equals(condition)) {
                valueParser.addMinIndexAgeCondition(value, setting);
            } else if (MinDocsCondition.NAME.equals(condition)) {
                valueParser.addMinIndexDocsCondition(value, setting);
            } else if (MinPrimaryShardDocsCondition.NAME.equals(condition)) {
                valueParser.addMinPrimaryShardDocsCondition(value, condition);
            } else {
                throw new SettingsException("Unknown condition: '{}'", condition);
            }
        }
        return valueParser.getRolloverConfiguration();
    }

    /**
     * When max_age is auto we’ll use the following retention dependent heuristics to compute the value of max_age:
     * - If retention is null aka infinite (default), max_age will be 30 days
     * - If retention is configured to anything lower than 14 days, max_age will be 1 day
     * - If retention is configured to anything lower than 90 days, max_age will be 7 days
     * - If retention is configured to anything greater than 90 days, max_age will be 30 days
     */
    static TimeValue evaluateMaxAgeCondition(@Nullable TimeValue retention) {
        if (retention == null) {
            return TimeValue.timeValueDays(30);
        }
        if (retention.compareTo(TimeValue.timeValueDays(14)) <= 0) {
            return TimeValue.timeValueDays(1);
        }
        if (retention.compareTo(TimeValue.timeValueDays(90)) <= 0) {
            return TimeValue.timeValueDays(7);
        }
        return TimeValue.timeValueDays(30);
    }

    /**
     * Parses and keeps track of the condition values during parsing
     */
    public static class ValueParser {
        private final RolloverConditions.Builder concreteConditions = RolloverConditions.newBuilder();
        private final Set<String> automatic = new HashSet<>();
        Set<String> encounteredConditions = new HashSet<>();

        /**
         * Parses and adds max index age condition (can be automatic)
         */
        public ValueParser addMaxIndexAgeCondition(String value, String setting) {
            if (value != null) {
                String condition = MaxAgeCondition.NAME;
                checkForDuplicatesAndAdd(condition, () -> {
                    if (value.equals("auto")) {
                        automatic.add(condition);
                    } else {
                        concreteConditions.addMaxIndexAgeCondition(TimeValue.parseTimeValue(value, setting));
                    }
                });
            }
            return this;
        }

        /**
         * Parses and adds max index docs condition
         */
        public ValueParser addMaxIndexDocsCondition(String value, String setting) {
            return addMaxIndexDocsCondition(parseLong(value, setting));
        }

        /**
         * Adds max index docs condition
         */
        public ValueParser addMaxIndexDocsCondition(Long maxDocs) {
            if (maxDocs != null) {
                String condition = MaxDocsCondition.NAME;
                checkForDuplicatesAndAdd(condition, () -> concreteConditions.addMaxIndexDocsCondition(maxDocs));
            }
            return this;
        }

        /**
         * Parses and adds max index size
         */
        public ValueParser addMaxIndexSizeCondition(String value, String setting) {
            if (value != null) {
                String condition = MaxSizeCondition.NAME;
                checkForDuplicatesAndAdd(
                    condition,
                    () -> concreteConditions.addMaxIndexSizeCondition(ByteSizeValue.parseBytesSizeValue(value, setting))
                );
            }
            return this;
        }

        /**
         * Parses and adds max index primary shard size
         */
        public ValueParser addMaxPrimaryShardSizeCondition(String value, String setting) {
            if (value != null) {
                String condition = MaxPrimaryShardSizeCondition.NAME;
                checkForDuplicatesAndAdd(
                    condition,
                    () -> concreteConditions.addMaxPrimaryShardSizeCondition(ByteSizeValue.parseBytesSizeValue(value, setting))
                );
            }
            return this;
        }

        /**
         * Parses and adds max primary shard doc count
         */
        public ValueParser addMaxPrimaryShardDocsCondition(String value, String setting) {
            return addMaxPrimaryShardDocsCondition(parseLong(value, setting));
        }

        /**
         * Adds max primary shard doc count
         */
        public ValueParser addMaxPrimaryShardDocsCondition(Long maxDocs) {
            if (maxDocs != null) {
                String condition = MaxPrimaryShardDocsCondition.NAME;
                checkForDuplicatesAndAdd(condition, () -> concreteConditions.addMaxPrimaryShardDocsCondition(maxDocs));
            }
            return this;
        }

        /**
         * Parses and adds min index age condition
         */
        public ValueParser addMinIndexAgeCondition(String value, String setting) {
            if (value != null) {
                String condition = MinAgeCondition.NAME;
                checkForDuplicatesAndAdd(
                    condition,
                    () -> concreteConditions.addMinIndexAgeCondition(TimeValue.parseTimeValue(value, setting))
                );
            }
            return this;
        }

        /**
         * Parses and adds the min index docs count condition
         */
        public ValueParser addMinIndexDocsCondition(String value, String setting) {
            return addMinIndexDocsCondition(parseLong(value, setting));
        }

        /**
         * Adds the min index docs count condition
         */
        public ValueParser addMinIndexDocsCondition(Long minDocs) {
            if (minDocs != null) {
                String condition = MinDocsCondition.NAME;
                checkForDuplicatesAndAdd(condition, () -> concreteConditions.addMinIndexDocsCondition(minDocs));
            }
            return this;
        }

        /**
         * Parses and adds min index size
         */
        public ValueParser addMinIndexSizeCondition(String value, String setting) {
            if (value != null) {
                String condition = MinSizeCondition.NAME;
                checkForDuplicatesAndAdd(
                    condition,
                    () -> concreteConditions.addMinIndexSizeCondition(ByteSizeValue.parseBytesSizeValue(value, setting))
                );
            }
            return this;
        }

        /**
         * Parses and adds min index primary shard size
         */
        public ValueParser addMinPrimaryShardSizeCondition(String value, String setting) {
            if (value != null) {
                String condition = MinPrimaryShardSizeCondition.NAME;
                checkForDuplicatesAndAdd(
                    condition,
                    () -> concreteConditions.addMinPrimaryShardSizeCondition(ByteSizeValue.parseBytesSizeValue(value, setting))
                );
            }
            return this;
        }

        /**
         * Parses and adds the max primary shard doc count
         */
        public ValueParser addMinPrimaryShardDocsCondition(String value, String setting) {
            return addMinPrimaryShardDocsCondition(parseLong(value, setting));
        }

        /**
         * Adds the max primary shard doc count
         */
        public ValueParser addMinPrimaryShardDocsCondition(Long minDocs) {
            if (minDocs != null) {
                String condition = MinPrimaryShardDocsCondition.NAME;
                checkForDuplicatesAndAdd(condition, () -> concreteConditions.addMinPrimaryShardDocsCondition(minDocs));
            }
            return this;
        }

        public void checkForDuplicatesAndAdd(String condition, Runnable parseAndAdd) {
            if (encounteredConditions.contains(condition)) {
                throw new IllegalArgumentException(condition + " condition is already set");
            }
            parseAndAdd.run();
            encounteredConditions.add(condition);
        }

        RolloverConfiguration getRolloverConfiguration() {
            return new RolloverConfiguration(concreteConditions.build(), Collections.unmodifiableSet(automatic));
        }

        private static Long parseLong(String sValue, String settingName) {
            try {
                return Long.parseLong(sValue);
            } catch (NumberFormatException e) {
                throw new SettingsException(
                    "Invalid value '{}' in setting '{}', the value is expected to be of type long",
                    sValue,
                    settingName,
                    e.getMessage()
                );
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RolloverConfiguration that = (RolloverConfiguration) o;
        return Objects.equals(concreteConditions, that.concreteConditions) && Objects.equals(automaticConditions, that.automaticConditions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(concreteConditions, automaticConditions);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
