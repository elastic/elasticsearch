/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.query;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.ProfileResultTests;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
        return new QueryProfileShardResult(queryProfileResults, rewriteTime, profileCollector);
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
        QueryProfileShardResult result = QueryProfileShardResult.fromXContent(parser);
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
