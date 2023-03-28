/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.application.search.SearchApplicationTestUtils.randomSearchApplicationQueryParams;

public class SearchApplicationQueryParamsSerializingTests extends AbstractWireSerializingTestCase<SearchApplicationQueryParams> {

    @Override
    protected Writeable.Reader<SearchApplicationQueryParams> instanceReader() {
        return SearchApplicationQueryParams::new;
    }

    @Override
    protected SearchApplicationQueryParams createTestInstance() {
        return randomSearchApplicationQueryParams();
    }

    @Override
    protected SearchApplicationQueryParams mutateInstance(SearchApplicationQueryParams instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }
}
