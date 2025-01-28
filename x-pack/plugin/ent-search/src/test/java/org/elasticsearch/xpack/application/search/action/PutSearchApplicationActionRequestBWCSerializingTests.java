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
import org.elasticsearch.xpack.application.search.SearchApplication;

import java.io.IOException;

public class PutSearchApplicationActionRequestBWCSerializingTests extends AbstractBWCSerializationTestCase<
    PutSearchApplicationAction.Request> {

    private String searchApplicationName;

    @Override
    protected Writeable.Reader<PutSearchApplicationAction.Request> instanceReader() {
        return PutSearchApplicationAction.Request::new;
    }

    @Override
    protected PutSearchApplicationAction.Request createTestInstance() {
        SearchApplication searchApp = EnterpriseSearchModuleTestUtils.randomSearchApplication();
        this.searchApplicationName = searchApp.name();
        return new PutSearchApplicationAction.Request(searchApp, randomBoolean());
    }

    @Override
    protected PutSearchApplicationAction.Request mutateInstance(PutSearchApplicationAction.Request instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected PutSearchApplicationAction.Request doParseInstance(XContentParser parser) throws IOException {
        return PutSearchApplicationAction.Request.parse(parser, this.searchApplicationName);
    }

    @Override
    protected PutSearchApplicationAction.Request mutateInstanceForVersion(
        PutSearchApplicationAction.Request instance,
        TransportVersion version
    ) {
        return instance;
    }
}
