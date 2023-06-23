/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Query simulating serialization error on versions earlier than CURRENT
 */
public class FailBeforeCurrentVersionQueryBuilder extends DummyQueryBuilder {

    public static final String NAME = "fail_before_current_version";

    public FailBeforeCurrentVersionQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    public FailBeforeCurrentVersionQueryBuilder() {}

    public static DummyQueryBuilder fromXContent(XContentParser parser) throws IOException {
        DummyQueryBuilder.fromXContent(parser);
        return new FailBeforeCurrentVersionQueryBuilder();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        return this;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        // this is what causes the failure - it always reports a version in the future, so it is never compatible with
        // current or minimum CCS TransportVersion
        return new TransportVersion(TransportVersion.current().id() + 11_111);
    }
}
