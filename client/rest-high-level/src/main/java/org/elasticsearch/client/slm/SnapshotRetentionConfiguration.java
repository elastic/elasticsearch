/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.slm;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class SnapshotRetentionConfiguration implements ToXContentObject {

    public static final SnapshotRetentionConfiguration EMPTY = new SnapshotRetentionConfiguration(null, null, null);

    private static final ParseField EXPIRE_AFTER = new ParseField("expire_after");
    private static final ParseField MINIMUM_SNAPSHOT_COUNT = new ParseField("min_count");
    private static final ParseField MAXIMUM_SNAPSHOT_COUNT = new ParseField("max_count");

    private static final ConstructingObjectParser<SnapshotRetentionConfiguration, Void> PARSER = new ConstructingObjectParser<>(
        "snapshot_retention",
        true,
        a -> {
            TimeValue expireAfter = a[0] == null ? null : TimeValue.parseTimeValue((String) a[0], EXPIRE_AFTER.getPreferredName());
            Integer minCount = (Integer) a[1];
            Integer maxCount = (Integer) a[2];
            return new SnapshotRetentionConfiguration(expireAfter, minCount, maxCount);
        }
    );

    static {
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), EXPIRE_AFTER);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), MINIMUM_SNAPSHOT_COUNT);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAXIMUM_SNAPSHOT_COUNT);
    }

    private final TimeValue expireAfter;
    private final Integer minimumSnapshotCount;
    private final Integer maximumSnapshotCount;

    public SnapshotRetentionConfiguration(
        @Nullable TimeValue expireAfter,
        @Nullable Integer minimumSnapshotCount,
        @Nullable Integer maximumSnapshotCount
    ) {
        this.expireAfter = expireAfter;
        this.minimumSnapshotCount = minimumSnapshotCount;
        this.maximumSnapshotCount = maximumSnapshotCount;
        if (this.minimumSnapshotCount != null && this.minimumSnapshotCount < 1) {
            throw new IllegalArgumentException("minimum snapshot count must be at least 1, but was: " + this.minimumSnapshotCount);
        }
        if (this.maximumSnapshotCount != null && this.maximumSnapshotCount < 1) {
            throw new IllegalArgumentException("maximum snapshot count must be at least 1, but was: " + this.maximumSnapshotCount);
        }
        if ((maximumSnapshotCount != null && minimumSnapshotCount != null) && this.minimumSnapshotCount > this.maximumSnapshotCount) {
            throw new IllegalArgumentException(
                "minimum snapshot count "
                    + this.minimumSnapshotCount
                    + " cannot be larger than maximum snapshot count "
                    + this.maximumSnapshotCount
            );
        }
    }

    public static SnapshotRetentionConfiguration parse(XContentParser parser, String name) {
        return PARSER.apply(parser, null);
    }

    public TimeValue getExpireAfter() {
        return this.expireAfter;
    }

    public Integer getMinimumSnapshotCount() {
        return this.minimumSnapshotCount;
    }

    public Integer getMaximumSnapshotCount() {
        return this.maximumSnapshotCount;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (expireAfter != null) {
            builder.field(EXPIRE_AFTER.getPreferredName(), expireAfter.getStringRep());
        }
        if (minimumSnapshotCount != null) {
            builder.field(MINIMUM_SNAPSHOT_COUNT.getPreferredName(), minimumSnapshotCount);
        }
        if (maximumSnapshotCount != null) {
            builder.field(MAXIMUM_SNAPSHOT_COUNT.getPreferredName(), maximumSnapshotCount);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(expireAfter, minimumSnapshotCount, maximumSnapshotCount);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        SnapshotRetentionConfiguration other = (SnapshotRetentionConfiguration) obj;
        return Objects.equals(this.expireAfter, other.expireAfter)
            && Objects.equals(minimumSnapshotCount, other.minimumSnapshotCount)
            && Objects.equals(maximumSnapshotCount, other.maximumSnapshotCount);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
