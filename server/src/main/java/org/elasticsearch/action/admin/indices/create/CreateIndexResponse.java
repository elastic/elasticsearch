/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.support.master.ShardsAcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A response for a create index action.
 */
public class CreateIndexResponse extends ShardsAcknowledgedResponse {

    public static final ParseField INDEX = new ParseField("index");
    public static final ParseField INDEX_LIMIT_TIER_MESSAGE = new ParseField("index_limit_tier_message");

    private final String index;

    private final IndexLimitTier indexLimitTier;

    public CreateIndexResponse(StreamInput in) throws IOException {
        super(in, true);
        index = in.readString();
        indexLimitTier = in.readEnum(IndexLimitTier.class);
    }

    public CreateIndexResponse(boolean acknowledged, boolean shardsAcknowledged, String index) {
        super(acknowledged, shardsAcknowledged);
        this.index = Objects.requireNonNull(index);
        this.indexLimitTier = IndexLimitTier.PASS;
    }

    public CreateIndexResponse(boolean acknowledged, boolean shardsAcknowledged, String index, IndexLimitTier indexLimitTier) {
        super(acknowledged, shardsAcknowledged);
        this.index = Objects.requireNonNull(index);
        this.indexLimitTier = indexLimitTier;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        writeShardsAcknowledged(out);
        out.writeString(index);
        out.writeEnum(indexLimitTier);
    }

    public String index() {
        return index;
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        super.addCustomFields(builder, params);
        builder.field(INDEX.getPreferredName(), index());
        builder.field(INDEX_LIMIT_TIER_MESSAGE.getPreferredName(), indexLimitTier.message);
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o)) {
            CreateIndexResponse that = (CreateIndexResponse) o;
            return Objects.equals(index, that.index);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), index);
    }

    public enum IndexLimitTier implements Writeable {
        PASS(0, 11_249, "Success"),
        NUDGE(11_250, 13_499, "Nudge"),
        WARN(13_500, 14_699, "Warn"),
        CRITICAL(14_701, 14_999, "Critical"),
        BLOCK(15_000, Integer.MAX_VALUE, "Blocked");

        private final int minInclusive;
        private final int maxInclusive;
        private final String message;

        IndexLimitTier(int minInclusive, int maxInclusive, String message) {
            this.minInclusive = minInclusive;
            this.maxInclusive = maxInclusive;
            this.message = message;
        }

        public boolean matches(int value) {
            return value >= minInclusive && value <= maxInclusive;
        }

        public boolean matches(String name) {
            return this.name().equals(name);
        }

        public static IndexLimitTier parse(int value) {
            for (IndexLimitTier tier : values()) {
                if (tier.matches(value)) {
                    return tier;
                }
            }
            throw new IllegalArgumentException("Value out of range: " + value);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }
    }
}
