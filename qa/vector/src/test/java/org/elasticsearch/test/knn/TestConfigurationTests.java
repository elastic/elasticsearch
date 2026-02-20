/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.knn;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.util.List;

public class TestConfigurationTests extends ESTestCase {

    public void testParameterParsing() throws Exception {
        String json = """
            {
              "doc_vectors": ["/path/to/docs"],
              "query_vectors": "/path/to/queries",
              "dimensions": 128,
              "num_candidates": [10, 20],
              "k": [5, 10],
              "visit_percentage": [0.5],
              "over_sampling_factor": [2.0],
              "search_threads": [1],
              "num_searchers": [1],
              "filter_selectivity": [0.8],
              "filter_cache": [true],
              "early_termination": [false],
              "seed": [123]
            }
            """;

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            TestConfiguration config = TestConfiguration.fromXContent(parser);
            assertEquals(128, config.dimensions());
            assertEquals(1, config.docVectors().size());
            assertTrue(config.docVectors().get(0).equals(PathUtils.get("/path/to/docs")));
            assertTrue(config.queryVectors().equals(PathUtils.get("/path/to/queries")));

            List<SearchParameters> params = config.searchParams();
            assertEquals(4, params.size());

            // Verify combinations
            // Order: numCandidates, k, ...
            // result = [[10, 5], [10, 10], [20, 5], [20, 10]]

            assertEquals(10, params.get(0).numCandidates());
            assertEquals(5, params.get(0).topK());

            assertEquals(10, params.get(1).numCandidates());
            assertEquals(10, params.get(1).topK());

            assertEquals(20, params.get(2).numCandidates());
            assertEquals(5, params.get(2).topK());

            assertEquals(20, params.get(3).numCandidates());
            assertEquals(10, params.get(3).topK());
        }
    }

}
