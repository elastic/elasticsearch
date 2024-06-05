/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.equalTo;

public class QueryShardExceptionTests extends ESTestCase {

    public void testCreateFromSearchExecutionContext() {
        String indexUuid = randomAlphaOfLengthBetween(5, 10);
        String clusterAlias = randomAlphaOfLengthBetween(5, 10);
        SearchExecutionContext searchExecutionContext = SearchExecutionContextTests.createSearchExecutionContext(indexUuid, clusterAlias);
        {
            QueryShardException queryShardException = new QueryShardException(searchExecutionContext, "error");
            assertThat(queryShardException.getIndex().getName(), equalTo(clusterAlias + ":index"));
            assertThat(queryShardException.getIndex().getUUID(), equalTo(indexUuid));
        }
        {
            QueryShardException queryShardException = new QueryShardException(
                searchExecutionContext,
                "error",
                new IllegalArgumentException()
            );
            assertThat(queryShardException.getIndex().getName(), equalTo(clusterAlias + ":index"));
            assertThat(queryShardException.getIndex().getUUID(), equalTo(indexUuid));
        }
    }

    public void testCreateFromIndex() {
        String indexUuid = randomAlphaOfLengthBetween(5, 10);
        String indexName = randomAlphaOfLengthBetween(5, 10);
        Index index = new Index(indexName, indexUuid);
        QueryShardException queryShardException = new QueryShardException(index, "error", new IllegalArgumentException());
        assertThat(queryShardException.getIndex().getName(), equalTo(indexName));
        assertThat(queryShardException.getIndex().getUUID(), equalTo(indexUuid));
    }
}
