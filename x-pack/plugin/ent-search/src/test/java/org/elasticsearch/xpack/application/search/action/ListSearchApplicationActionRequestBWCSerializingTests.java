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
import org.elasticsearch.xpack.core.action.util.PageParams;

import java.io.IOException;

public class ListSearchApplicationActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    ListSearchApplicationAction.Request> {

    @Override
    protected Writeable.Reader<ListSearchApplicationAction.Request> instanceReader() {
        return ListSearchApplicationAction.Request::new;
    }

    @Override
    protected ListSearchApplicationAction.Request createTestInstance() {

        PageParams pageParams = EnterpriseSearchModuleTestUtils.randomPageParams();
        String query = randomFrom(new String[] { null, randomAlphaOfLengthBetween(1, 10) });
        return new ListSearchApplicationAction.Request(query, pageParams);
    }

    @Override
    protected ListSearchApplicationAction.Request mutateInstance(ListSearchApplicationAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected ListSearchApplicationAction.Request doParseInstance(XContentParser parser) throws IOException {
        return ListSearchApplicationAction.Request.parse(parser);
    }

    @Override
    protected ListSearchApplicationAction.Request mutateInstanceForVersion(
        ListSearchApplicationAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
