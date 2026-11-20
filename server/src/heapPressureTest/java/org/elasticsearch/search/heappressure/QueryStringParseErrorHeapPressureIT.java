/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.heappressure;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * A malformed {@code query_string} fails to parse on every shard, and the search keeps each shard failure until it ends. The parse error
 * repeats the query, and a shard failure keeps the stack trace of the error, so the query must not be copied in full into each failure.
 * The documents are small: the memory is all in the errors.
 */
public class QueryStringParseErrorHeapPressureIT extends SearchHeapPressureTestCase {

    private static final String INDEX = "parse-errors";
    private static final int SHARDS = 40;
    /** A failure that copies the query four times holds about 8 MB, so the failures of all shards are more than the heap. */
    private static final int QUERY_CHARS = 2 * 1024 * 1024;

    @ClassRule
    public static ElasticsearchCluster cluster = heapConstrainedNode("256m").build();

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    @Before
    public void indexKeywords() throws IOException {
        indexDocuments(INDEX, SHARDS, """
            {"f": {"type": "keyword"}}""", SHARDS, i -> Strings.format("{\"f\":\"value-%04d\"}", i));
    }

    /** The leading {@code )} fails the parse at the first token, so the shards do not build anything else from the query. */
    public void testLargeMalformedQueryFailsWithoutRunningOutOfMemory() throws IOException {
        String query = ")" + "a".repeat(QUERY_CHARS);
        Map<String, Object> body = errorBodyOrNull(search(INDEX, Strings.format("""
            {"query": {"query_string": {"query": "%s", "default_field": "f"}}}""", query)));
        assertNotNull("expected the malformed query to fail", body);
        assertThat(body.get("status"), equalTo(400));
        assertNodeAlive();
    }
}
