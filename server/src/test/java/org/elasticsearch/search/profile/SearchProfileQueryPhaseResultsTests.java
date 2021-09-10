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

import java.util.HashMap;
import java.util.Map;

public class SearchProfileQueryPhaseResultsTests extends AbstractWireSerializingTestCase<SearchProfileResultsBuilder> {
    @Override
    protected SearchProfileResultsBuilder createTestInstance() {
        int size = rarely() ? 0 : randomIntBetween(1, 2);
        Map<String, SearchProfileQueryPhaseResult> shards = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            shards.put(randomAlphaOfLengthBetween(5, 10), SearchProfileQueryPhaseResultTests.createTestItem());
        }
        return new SearchProfileResultsBuilder(shards);
    }

    @Override
    protected Reader<SearchProfileResultsBuilder> instanceReader() {
        return SearchProfileResultsBuilder::new;
    }
}
