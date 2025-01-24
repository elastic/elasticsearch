/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.EnterpriseSearchModuleTestUtils;

import java.io.IOException;

public class SearchApplicationSearchRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<SearchApplicationSearchRequest> {

    @Override
    protected Writeable.Reader<SearchApplicationSearchRequest> instanceReader() {
        return SearchApplicationSearchRequest::new;
    }

    @Override
    protected SearchApplicationSearchRequest createTestInstance() {
        return new SearchApplicationSearchRequest(
            randomAlphaOfLengthBetween(1, 10),
            EnterpriseSearchModuleTestUtils.randomSearchApplicationQueryParams()
        );
    }

    @Override
    protected SearchApplicationSearchRequest mutateInstance(SearchApplicationSearchRequest instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected SearchApplicationSearchRequest doParseInstance(XContentParser parser) throws IOException {
        return SearchApplicationSearchRequest.parse(parser);
    }

    @Override
    protected SearchApplicationSearchRequest mutateInstanceForVersion(SearchApplicationSearchRequest instance, TransportVersion version) {
        return new SearchApplicationSearchRequest(instance.name(), instance.queryParams());
    }

}
