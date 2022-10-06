/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.profile.query.QueryProfileShardResultTests;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class SearchProfileDfsPhaseResultTests extends AbstractSerializingTestCase<SearchProfileDfsPhaseResult> {

    static SearchProfileDfsPhaseResult createTestItem() {
        return new SearchProfileDfsPhaseResult(
            randomBoolean() ? null : ProfileResultTests.createTestItem(1),
            randomBoolean() ? null : QueryProfileShardResultTests.createTestItem()
        );
    }

    @Override
    protected SearchProfileDfsPhaseResult createTestInstance() {
        return createTestItem();
    }

    @Override
    protected Reader<SearchProfileDfsPhaseResult> instanceReader() {
        return SearchProfileDfsPhaseResult::new;
    }

    @Override
    protected SearchProfileDfsPhaseResult doParseInstance(XContentParser parser) throws IOException {
        return SearchProfileDfsPhaseResult.fromXContent(parser);
    }
}
