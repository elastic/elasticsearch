/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.rerank;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class RerankingRankBuilder extends RankBuilder {

    public static final ParseField FIELD_FIELD = new ParseField("field");
    static final ConstructingObjectParser<RerankingRankBuilder, Void> PARSER = new ConstructingObjectParser<>("rerank", args -> {
        int rankWindowSize = args[0] == null ? DEFAULT_RANK_WINDOW_SIZE : (int) args[0];
        String field = (String) args[1];
        if (field == null || field.isEmpty()) {
            throw new IllegalArgumentException("Field cannot be null or empty");
        }
        return new RerankingRankBuilder(rankWindowSize, field);
    });

    static {
        PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
        PARSER.declareString(constructorArg(), FIELD_FIELD);
    }

    private final String field;

    public static RerankingRankBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public RerankingRankBuilder(int rankWindowSize, String field) {
        super(rankWindowSize);
        this.field = field;
    }

    public RerankingRankBuilder(StreamInput in) throws IOException {
        super(in);
        this.field = in.readString();
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELD_FIELD.getPreferredName(), field);
    }

    @Override
    public boolean isCompoundBuilder() {
        return false;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.current();
    }

    @Override
    public String getWriteableName() {
        return "rerank";
    }

    @Override
    public RerankingQueryPhaseRankShardContext buildQueryPhaseShardContext(List<Query> queries, int from) {
        return new RerankingQueryPhaseRankShardContext(queries, rankWindowSize());
    }

    @Override
    public RerankingQueryPhaseRankCoordinatorContext buildQueryPhaseCoordinatorContext(int size, int from) {
        return new RerankingQueryPhaseRankCoordinatorContext(rankWindowSize());
    }

    @Override
    public RerankingRankFeaturePhaseRankShardContext buildRankFeaturePhaseShardContext() {
        return new RerankingRankFeaturePhaseRankShardContext(field);
    }

    @Override
    public RerankingRankFeaturePhaseRankCoordinatorContext buildRankFeaturePhaseCoordinatorContext(int size, int from, Client client) {
        return new RandomOrderRankFeaturePhaseRankCoordinatorContext(size, from, rankWindowSize());
    }

    @Override
    protected boolean doEquals(RankBuilder other) {
        return Objects.equals(field, ((RerankingRankBuilder) other).field);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field);
    }
}
