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
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.test.ESTestCase.frequently;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class TestRankBuilder extends RankBuilder<TestRankBuilder> {

    public static final String NAME = "rank_test";

    public static TestRankBuilder randomRankBuilder() {
        TestRankBuilder testRankBuilder = new TestRankBuilder();
        if (frequently()) {
            testRankBuilder.windowSize(randomIntBetween(0, 10000));
        }
        return testRankBuilder;
    }

    public TestRankBuilder() {
        super();
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
    public ActionRequestValidationException validate(ActionRequestValidationException validationException, SearchSourceBuilder source) {
        return validationException;
    }

    @Override
    public RankShardContext build(List<Query> queries, int size, int from) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RankCoordinatorContext build(int size, int from) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean doEquals(TestRankBuilder other) {
        return true;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }
}
