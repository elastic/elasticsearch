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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Contains the conditions that determine if an index can be rolled over or not. It is used by the {@link RolloverRequest},
 * the Index Lifecycle Management and the Data Stream Lifecycle.
 */
public class RolloverConditions implements Writeable, ToXContentObject {
    public static final ObjectParser<RolloverConditions.Builder, Void> PARSER = new ObjectParser<>("rollover_conditions");
    public static final ParseField MAX_AGE_FIELD = new ParseField(MaxAgeCondition.NAME);
    public static final ParseField MAX_DOCS_FIELD = new ParseField(MaxDocsCondition.NAME);
    public static final ParseField MAX_SIZE_FIELD = new ParseField(MaxSizeCondition.NAME);
    public static final ParseField MAX_PRIMARY_SHARD_SIZE_FIELD = new ParseField(MaxPrimaryShardSizeCondition.NAME);
    public static final ParseField MAX_PRIMARY_SHARD_DOCS_FIELD = new ParseField(MaxPrimaryShardDocsCondition.NAME);
    public static final ParseField MIN_AGE_FIELD = new ParseField(MinAgeCondition.NAME);
    public static final ParseField MIN_DOCS_FIELD = new ParseField(MinDocsCondition.NAME);
    public static final ParseField MIN_SIZE_FIELD = new ParseField(MinSizeCondition.NAME);
    public static final ParseField MIN_PRIMARY_SHARD_SIZE_FIELD = new ParseField(MinPrimaryShardSizeCondition.NAME);
    public static final ParseField MIN_PRIMARY_SHARD_DOCS_FIELD = new ParseField(MinPrimaryShardDocsCondition.NAME);

    static {
        PARSER.declareString(
            (builder, s) -> builder.addMaxIndexAgeCondition(TimeValue.parseTimeValue(s, MaxAgeCondition.NAME)),
            MAX_AGE_FIELD
        );
        PARSER.declareLong(RolloverConditions.Builder::addMaxIndexDocsCondition, MAX_DOCS_FIELD);
        PARSER.declareString(
            (builder, s) -> builder.addMaxIndexSizeCondition(ByteSizeValue.parseBytesSizeValue(s, MaxSizeCondition.NAME)),
            MAX_SIZE_FIELD
        );
        PARSER.declareString(
            (builder, s) -> builder.addMaxPrimaryShardSizeCondition(
                ByteSizeValue.parseBytesSizeValue(s, MaxPrimaryShardSizeCondition.NAME)
            ),
            MAX_PRIMARY_SHARD_SIZE_FIELD
        );
        PARSER.declareLong(RolloverConditions.Builder::addMaxPrimaryShardDocsCondition, MAX_PRIMARY_SHARD_DOCS_FIELD);
        PARSER.declareString(
            (builder, s) -> builder.addMinIndexAgeCondition(TimeValue.parseTimeValue(s, MinAgeCondition.NAME)),
            MIN_AGE_FIELD
        );
        PARSER.declareLong(RolloverConditions.Builder::addMinIndexDocsCondition, MIN_DOCS_FIELD);
        PARSER.declareString(
            (builder, s) -> builder.addMinIndexSizeCondition(ByteSizeValue.parseBytesSizeValue(s, MinSizeCondition.NAME)),
            MIN_SIZE_FIELD
        );
        PARSER.declareString(
            (builder, s) -> builder.addMinPrimaryShardSizeCondition(
                ByteSizeValue.parseBytesSizeValue(s, MinPrimaryShardSizeCondition.NAME)
            ),
            MIN_PRIMARY_SHARD_SIZE_FIELD
        );
        PARSER.declareLong(RolloverConditions.Builder::addMinPrimaryShardDocsCondition, MIN_PRIMARY_SHARD_DOCS_FIELD);
    }

    private final Map<String, Condition<?>> conditions;

    public RolloverConditions() {
        conditions = Map.of();
    }

    public RolloverConditions(StreamInput in) throws IOException {
        int size = in.readVInt();
        Map<String, Condition<?>> conditions = Maps.newMapWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
            Condition<?> condition = in.readNamedWriteable(Condition.class);
            conditions.put(condition.name, condition);
        }
        this.conditions = Collections.unmodifiableMap(conditions);
    }

    public RolloverConditions(Map<String, Condition<?>> conditions) {
        this.conditions = Collections.unmodifiableMap(conditions);
    }

    /**
     * Returns true if there is at least one condition of type MAX
     */
    public boolean hasMaxConditions() {
        return conditions.values().stream().anyMatch(c -> Condition.Type.MAX == c.type());
    }

    /**
     * Returns true if there is at least one condition of type MIN
     */
    public boolean hasMinConditions() {
        return conditions.values().stream().anyMatch(c -> Condition.Type.MIN == c.type());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(
            conditions.values().stream().filter(c -> c.includedInVersion(out.getTransportVersion())).toList(),
            StreamOutput::writeNamedWriteable
        );
    }

    /**
     * Returns the size an index can reach before rollover is triggered, if defined, or null otherwise
     */
    @Nullable
    public ByteSizeValue getMaxSize() {
        return conditions.containsKey(MaxSizeCondition.NAME) ? (ByteSizeValue) conditions.get(MaxSizeCondition.NAME).value() : null;
    }

    /**
     * Returns the size a primary shard can reach before rollover is triggered, if defined, or null otherwise
     */
    @Nullable
    public ByteSizeValue getMaxPrimaryShardSize() {
        return conditions.containsKey(MaxPrimaryShardSizeCondition.NAME)
            ? (ByteSizeValue) conditions.get(MaxPrimaryShardSizeCondition.NAME).value()
            : null;
    }

    /**
     * Returns the age an index can reach before rollover is triggered, if defined, or null otherwise
     */
    @Nullable
    public TimeValue getMaxAge() {
        return conditions.containsKey(MaxAgeCondition.NAME) ? (TimeValue) conditions.get(MaxAgeCondition.NAME).value() : null;
    }

    /**
     * Returns the max count of documents an index can reach before rollover is triggered, if it is defined, or null otherwise
     */
    @Nullable
    public Long getMaxDocs() {
        return conditions.containsKey(MaxDocsCondition.NAME) ? (Long) conditions.get(MaxDocsCondition.NAME).value() : null;
    }

    /**
     * Returns the max count of documents a primary shard can reach before rollover is triggered, if it is defined, or null otherwise
     */
    @Nullable
    public Long getMaxPrimaryShardDocs() {
        return conditions.containsKey(MaxPrimaryShardDocsCondition.NAME)
            ? (Long) conditions.get(MaxPrimaryShardDocsCondition.NAME).value()
            : null;
    }

    /**
     * Returns the minimum size an index is required to have before rollover is allowed, if it is defined, or null otherwise
     */
    @Nullable
    public ByteSizeValue getMinSize() {
        return conditions.containsKey(MinSizeCondition.NAME) ? (ByteSizeValue) conditions.get(MinSizeCondition.NAME).value() : null;
    }

    /**
     * Returns the minimum size a primary shard is required to have before rollover is allowed, if it is defined, or null otherwise
     */
    @Nullable
    public ByteSizeValue getMinPrimaryShardSize() {
        return conditions.containsKey(MinPrimaryShardSizeCondition.NAME)
            ? (ByteSizeValue) conditions.get(MinPrimaryShardSizeCondition.NAME).value()
            : null;
    }

    /**
     * Returns the minimum age an index is required to have before rollover is allowed, if it is defined, or null otherwise
     */
    @Nullable
    public TimeValue getMinAge() {
        return conditions.containsKey(MinAgeCondition.NAME) ? (TimeValue) conditions.get(MinAgeCondition.NAME).value() : null;
    }

    /**
     * Returns the minimum document count an index is required to have before rollover is allowed, if it is defined, or null otherwise
     */
    @Nullable
    public Long getMinDocs() {
        return conditions.containsKey(MinDocsCondition.NAME) ? (Long) conditions.get(MinDocsCondition.NAME).value() : null;
    }

    /**
     * Returns the minimum document count a primary shard is required to have before rollover is allowed, if it is defined,
     * or null otherwise
     */
    @Nullable
    public Long getMinPrimaryShardDocs() {
        return conditions.containsKey(MinPrimaryShardDocsCondition.NAME)
            ? (Long) conditions.get(MinPrimaryShardDocsCondition.NAME).value()
            : null;
    }

    public Map<String, Condition<?>> getConditions() {
        return conditions;
    }

    /**
     * Given the results of evaluating each individual condition, determine whether the rollover request should proceed -- that is,
     * whether the conditions are met.
     *
     * If there are no conditions at all, then the request is unconditional (i.e. a command), and the conditions are met.
     *
     * If the request has conditions, then all min_* conditions and at least one max_* condition must have a true result.
     *
     * @param conditionResults a map of individual conditions and their associated evaluation results
     *
     * @return where the conditions for rollover are satisfied or not
     */
    public boolean areConditionsMet(Map<String, Boolean> conditionResults) {
        boolean allMinConditionsMet = conditions.values()
            .stream()
            .filter(c -> Condition.Type.MIN == c.type())
            .allMatch(c -> conditionResults.getOrDefault(c.toString(), false));

        boolean anyMaxConditionsMet = conditions.values()
            .stream()
            .filter(c -> Condition.Type.MAX == c.type())
            .anyMatch(c -> conditionResults.getOrDefault(c.toString(), false));

        return conditionResults.size() == 0 || (allMinConditionsMet && anyMaxConditionsMet);
    }

    public static RolloverConditions fromXContent(XContentParser parser) throws IOException {
        RolloverConditions.Builder builder = RolloverConditions.newBuilder();
        PARSER.parse(parser, builder, null);
        return builder.build();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragment(builder, params);
        builder.endObject();
        return builder;
    }

    /**
     * This method adds the conditions as fields in an already existing object.
     */
    public XContentBuilder toXContentFragment(XContentBuilder builder, Params params) throws IOException {
        for (Condition<?> condition : conditions.values()) {
            condition.toXContent(builder, params);
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RolloverConditions that = (RolloverConditions) o;
        return Objects.equals(conditions, that.conditions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(conditions);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * Helps to build or create a mutation of rollover conditions
     */
    public static class Builder {
        private final Map<String, Condition<?>> conditions;

        private Builder(Map<String, Condition<?>> conditions) {
            this.conditions = conditions;
        }

        /**
         * Adds condition to check if the index is at least <code>age</code> old
         */
        public Builder addMaxIndexAgeCondition(TimeValue age) {
            if (age != null) {
                MaxAgeCondition maxAgeCondition = new MaxAgeCondition(age);
                this.conditions.put(maxAgeCondition.name, maxAgeCondition);
            }
            return this;
        }

        /**
         * Adds condition to check if the index has at least <code>numDocs</code>
         */
        public Builder addMaxIndexDocsCondition(Long numDocs) {
            if (numDocs != null) {
                MaxDocsCondition maxDocsCondition = new MaxDocsCondition(numDocs);
                this.conditions.put(maxDocsCondition.name, maxDocsCondition);
            }
            return this;
        }

        /**
         * Adds a size-based condition to check if the index size is at least <code>size</code>.
         */
        public Builder addMaxIndexSizeCondition(ByteSizeValue size) {
            if (size != null) {
                MaxSizeCondition maxSizeCondition = new MaxSizeCondition(size);
                this.conditions.put(maxSizeCondition.name, maxSizeCondition);
            }
            return this;
        }

        /**
         * Adds a size-based condition to check if the size of the largest primary shard is at least <code>size</code>.
         */
        public Builder addMaxPrimaryShardSizeCondition(ByteSizeValue size) {
            if (size != null) {
                MaxPrimaryShardSizeCondition maxPrimaryShardSizeCondition = new MaxPrimaryShardSizeCondition(size);
                this.conditions.put(maxPrimaryShardSizeCondition.name, maxPrimaryShardSizeCondition);
            }
            return this;
        }

        /**
         * Adds a size-based condition to check if the docs of the largest primary shard has at least <code>numDocs</code>
         */
        public Builder addMaxPrimaryShardDocsCondition(Long numDocs) {
            if (numDocs != null) {
                MaxPrimaryShardDocsCondition maxPrimaryShardDocsCondition = new MaxPrimaryShardDocsCondition(numDocs);
                this.conditions.put(maxPrimaryShardDocsCondition.name, maxPrimaryShardDocsCondition);
            }
            return this;
        }

        /**
         * Adds required condition to check if the index is at least <code>age</code> old
         */
        public Builder addMinIndexAgeCondition(TimeValue age) {
            if (age != null) {
                MinAgeCondition minAgeCondition = new MinAgeCondition(age);
                this.conditions.put(minAgeCondition.name, minAgeCondition);
            }
            return this;
        }

        /**
         * Adds required condition to check if the index has at least <code>numDocs</code>
         */
        public Builder addMinIndexDocsCondition(Long numDocs) {
            if (numDocs != null) {
                MinDocsCondition minDocsCondition = new MinDocsCondition(numDocs);
                this.conditions.put(minDocsCondition.name, minDocsCondition);
            }
            return this;
        }

        /**
         * Adds a size-based required condition to check if the index size is at least <code>size</code>.
         */
        public Builder addMinIndexSizeCondition(ByteSizeValue size) {
            if (size != null) {
                MinSizeCondition minSizeCondition = new MinSizeCondition(size);
                this.conditions.put(minSizeCondition.name, minSizeCondition);
            }
            return this;
        }

        /**
         * Adds a size-based required condition to check if the size of the largest primary shard is at least <code>size</code>.
         */
        public Builder addMinPrimaryShardSizeCondition(ByteSizeValue size) {
            if (size != null) {
                MinPrimaryShardSizeCondition minPrimaryShardSizeCondition = new MinPrimaryShardSizeCondition(size);
                this.conditions.put(minPrimaryShardSizeCondition.name, minPrimaryShardSizeCondition);
            }
            return this;
        }

        /**
         * Adds a size-based required condition to check if the docs of the largest primary shard has at least <code>numDocs</code>
         */
        public Builder addMinPrimaryShardDocsCondition(Long numDocs) {
            if (numDocs != null) {
                MinPrimaryShardDocsCondition minPrimaryShardDocsCondition = new MinPrimaryShardDocsCondition(numDocs);
                this.conditions.put(minPrimaryShardDocsCondition.name, minPrimaryShardDocsCondition);
            }
            return this;
        }

        public RolloverConditions build() {
            return new RolloverConditions(conditions);
        }
    }

    public static RolloverConditions.Builder newBuilder(RolloverConditions conditions) {
        return new Builder(new HashMap<>(conditions.conditions));
    }

    public static RolloverConditions.Builder newBuilder() {
        return new Builder(new HashMap<>());
    }

}
