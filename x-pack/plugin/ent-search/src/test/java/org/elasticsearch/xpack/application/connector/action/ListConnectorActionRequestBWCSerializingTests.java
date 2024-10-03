/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.connector.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.EnterpriseSearchModuleTestUtils;
import org.elasticsearch.xpack.core.action.util.PageParams;

import java.io.IOException;
import java.util.List;

public class ListConnectorActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<ListConnectorAction.Request> {
    @Override
    protected Writeable.Reader<ListConnectorAction.Request> instanceReader() {
        return ListConnectorAction.Request::new;
    }

    @Override
    protected ListConnectorAction.Request createTestInstance() {
        PageParams pageParams = EnterpriseSearchModuleTestUtils.randomPageParams();
        return new ListConnectorAction.Request(
            pageParams,
            List.of(generateRandomStringArray(10, 10, false)),
            List.of(generateRandomStringArray(10, 10, false)),
            List.of(generateRandomStringArray(10, 10, false)),
            randomAlphaOfLengthBetween(3, 10)
        );
    }

    @Override
    protected ListConnectorAction.Request mutateInstance(ListConnectorAction.Request instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected ListConnectorAction.Request doParseInstance(XContentParser parser) throws IOException {
        return ListConnectorAction.Request.parse(parser);
    }

    @Override
    protected ListConnectorAction.Request mutateInstanceForVersion(ListConnectorAction.Request instance, TransportVersion version) {
        return new ListConnectorAction.Request(
            instance.getPageParams(),
            instance.getIndexNames(),
            instance.getConnectorNames(),
            instance.getConnectorServiceTypes(),
            instance.getConnectorSearchQuery()
        );
    }
}
