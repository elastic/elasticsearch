/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.plugin.DummyQueryBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Query simulating a new named writable introduced in the current version.
 */
public class NewlyReleasedQueryBuilder extends DummyQueryBuilder {

    static final String NAME = "new_released_query";

    public NewlyReleasedQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    public NewlyReleasedQueryBuilder() {}

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public static NewlyReleasedQueryBuilder fromXContent(XContentParser parser) throws IOException {
        DummyQueryBuilder.fromXContent(parser);
        return new NewlyReleasedQueryBuilder();
    }

    @Override
    public Version getFirstReleasedVersion() {
        return Version.CURRENT;
    }
}