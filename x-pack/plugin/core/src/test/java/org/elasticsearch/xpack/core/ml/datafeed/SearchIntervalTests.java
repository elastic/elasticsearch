/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class SearchIntervalTests extends AbstractWireSerializingTestCase<SearchInterval> {

    @Override
    protected Writeable.Reader<SearchInterval> instanceReader() {
        return SearchInterval::new;
    }

    @Override
    protected SearchInterval createTestInstance() {
        return createRandom();
    }

    @Override
    protected SearchInterval mutateInstance(SearchInterval instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static SearchInterval createRandom() {
        long start = randomNonNegativeLong();
        return new SearchInterval(start, randomLongBetween(start, Long.MAX_VALUE));
    }
}
