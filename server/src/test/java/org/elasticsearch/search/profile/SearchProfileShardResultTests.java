/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class SearchProfileShardResultTests extends AbstractWireSerializingTestCase<SearchProfileShardResult> {
    static SearchProfileShardResult createTestItem() {
        SearchProfileQueryPhaseResult searchResult = SearchProfileQueryPhaseResultTests.createTestItem();
        ProfileResult fetchResult = randomBoolean() ? null : ProfileResultTests.createTestItem(2);
        return new SearchProfileShardResult(searchResult, fetchResult);
    }

    @Override
    protected SearchProfileShardResult createTestInstance() {
        return createTestItem();
    }

    @Override
    protected Reader<SearchProfileShardResult> instanceReader() {
        return SearchProfileShardResult::new;
    }
}
