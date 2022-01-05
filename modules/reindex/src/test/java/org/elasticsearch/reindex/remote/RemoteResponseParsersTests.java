/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex.remote;

import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.hamcrest.Matchers;

import java.io.IOException;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class RemoteResponseParsersTests extends ESTestCase {

    /**
     * Check that we can parse shard search failures without index information.
     */
    public void testFailureWithoutIndex() throws IOException {
        ShardSearchFailure failure = new ShardSearchFailure(new EsRejectedExecutionException("exhausted"));
        XContentBuilder builder = jsonBuilder();
        failure.toXContent(builder, ToXContent.EMPTY_PARAMS);
        try (XContentParser parser = createParser(builder)) {
            ScrollableHitSource.SearchFailure parsed = RemoteResponseParsers.SEARCH_FAILURE_PARSER.parse(parser, null);
            assertNotNull(parsed.getReason());
            assertThat(parsed.getReason().getMessage(), Matchers.containsString("exhausted"));
            assertThat(parsed.getReason(), Matchers.instanceOf(EsRejectedExecutionException.class));
        }
    }
}
