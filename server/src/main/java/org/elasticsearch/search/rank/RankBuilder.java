/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * {@code RankContextBuilder} is used as a base class to manage input, parsing,
 * and subsequent generation of appropriate contexts for handling searches that
 * require multiple queries for global rank relevance.
 */
public abstract class RankBuilder implements VersionedNamedWriteable, ToXContentObject {

    public static final ParseField WINDOW_SIZE_FIELD = new ParseField("window_size");

    public static final int DEFAULT_WINDOW_SIZE = SearchService.DEFAULT_SIZE;

    private final int windowSize;

    public RankBuilder(int windowSize) {
        this.windowSize = windowSize;
    }

    public RankBuilder(StreamInput in) throws IOException {
        windowSize = in.readVInt();
    }

    public final void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(windowSize);
        doWriteTo(out);
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(getWriteableName());
        builder.field(WINDOW_SIZE_FIELD.getPreferredName(), windowSize);
        doXContent(builder, params);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    protected abstract void doXContent(XContentBuilder builder, Params params) throws IOException;

    public int windowSize() {
        return windowSize;
    }

    /**
     * Generates a context used to execute required searches on the shard.
     */
    public abstract RankShardContext buildRankShardContext(List<Query> queries, int from);

    /**
     * Generates a context used to perform global ranking on the coordinator.
     */
    public abstract RankCoordinatorContext buildRankCoordinatorContext(int size, int from);

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked")
        RankBuilder other = (RankBuilder) obj;
        return Objects.equals(windowSize, other.windowSize()) && doEquals(other);
    }

    protected abstract boolean doEquals(RankBuilder other);

    @Override
    public final int hashCode() {
        return Objects.hash(getClass(), windowSize, doHashCode());
    }

    protected abstract int doHashCode();

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
