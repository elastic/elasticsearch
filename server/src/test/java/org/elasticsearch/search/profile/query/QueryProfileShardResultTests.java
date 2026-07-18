/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.query;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.ProfileResultTests;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class QueryProfileShardResultTests extends AbstractXContentSerializingTestCase<QueryProfileShardResult> {
    public static QueryProfileShardResult createTestItem() {
        int size = randomIntBetween(0, 5);
        List<ProfileResult> queryProfileResults = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            queryProfileResults.add(ProfileResultTests.createTestItem(1));
        }
        CollectorResult profileCollector = CollectorResultTests.createTestItem(2);
        long rewriteTime = randomNonNegativeLong();
        if (randomBoolean()) {
            rewriteTime = rewriteTime % 1000; // make sure to often test this with small values too
        }

        Long vectorOperationsCount = randomBoolean() ? null : randomNonNegativeLong();
        Map<String, Object> knnProfileBreakdown = randomBoolean() ? null : createRandomKnnProfile();
        return new QueryProfileShardResult(queryProfileResults, rewriteTime, profileCollector, vectorOperationsCount, knnProfileBreakdown);
    }

    static Map<String, Object> createRandomKnnProfile() {
        Map<String, Object> map = new LinkedHashMap<>();
        String algorithm = randomFrom("ivf", "hnsw");
        map.put("algorithm", algorithm);
        map.put("total_time_ns", randomNonNegativeLong());
        map.put("segments_searched", randomIntBetween(0, 20));
        map.put("early_terminated", randomBoolean());
        if ("ivf".equals(algorithm)) {
            Map<String, Object> ivf = new LinkedHashMap<>();
            ivf.put("centroids_evaluated", randomIntBetween(1, 100));
            ivf.put("postings_scored", randomNonNegativeLong());
            Map<String, Object> timings = new LinkedHashMap<>();
            timings.put("centroid_iterator_create_ns", randomNonNegativeLong());
            timings.put("posting_visit_ns", randomNonNegativeLong());
            timings.put("scoring_ns", randomNonNegativeLong());
            ivf.put("timings", timings);
            map.put("ivf", ivf);
        } else {
            Map<String, Object> hnsw = new LinkedHashMap<>();
            hnsw.put("k", randomIntBetween(1, 100));
            hnsw.put("num_candidates", randomIntBetween(10, 500));
            hnsw.put("nodes_visited", randomNonNegativeLong());
            Map<String, Object> timings = new LinkedHashMap<>();
            timings.put("sum_leaf_search_ns", randomNonNegativeLong());
            timings.put("merge_ns", randomNonNegativeLong());
            hnsw.put("timings", timings);
            map.put("hnsw", hnsw);
        }
        return map;
    }

    @Override
    protected QueryProfileShardResult createTestInstance() {
        return createTestItem();
    }

    @Override
    protected QueryProfileShardResult mutateInstance(QueryProfileShardResult instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected QueryProfileShardResult doParseInstance(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        QueryProfileShardResult result = SearchResponseUtils.parseQueryProfileShardResult(parser);
        ensureExpectedToken(null, parser.nextToken(), parser);
        return result;
    }

    @Override
    protected Reader<QueryProfileShardResult> instanceReader() {
        return QueryProfileShardResult::new;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return ProfileResultTests.RANDOM_FIELDS_EXCLUDE_FILTER;
    }
}
