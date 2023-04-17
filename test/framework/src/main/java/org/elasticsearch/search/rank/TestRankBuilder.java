/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TestRankBuilder extends RankBuilder {

    public static final String NAME = "rank_test";

    static final ConstructingObjectParser<TestRankBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        args -> new TestRankBuilder(args[0] == null ? DEFAULT_WINDOW_SIZE : (int) args[0])
    );

    static {
        PARSER.declareInt(optionalConstructorArg(), WINDOW_SIZE_FIELD);
    }

    public static TestRankBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static TestRankBuilder randomRankBuilder() {
        return new TestRankBuilder(randomIntBetween(0, 100000));
    }

    public TestRankBuilder(int windowSize) {
        super(windowSize);
    }

    public TestRankBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_8_0;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        // do nothing
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        // do nothing
    }

    @Override
    public RankShardContext buildRankShardContext(List<Query> queries, int from) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RankCoordinatorContext buildRankCoordinatorContext(int size, int from) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean doEquals(RankBuilder other) {
        return true;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }
}
