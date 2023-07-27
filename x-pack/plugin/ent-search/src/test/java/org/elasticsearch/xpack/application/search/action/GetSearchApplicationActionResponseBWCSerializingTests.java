/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.search.SearchApplication;
import org.elasticsearch.xpack.application.search.SearchApplicationTestUtils;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;

import java.io.IOException;

public class GetSearchApplicationActionResponseBWCSerializingTests extends AbstractBWCSerializationTestCase<
    GetSearchApplicationAction.Response> {

    private String searchApplicationName;

    @Override
    protected Writeable.Reader<GetSearchApplicationAction.Response> instanceReader() {
        return GetSearchApplicationAction.Response::new;
    }

    @Override
    protected GetSearchApplicationAction.Response createTestInstance() {
        SearchApplication searchApp = SearchApplicationTestUtils.randomSearchApplication();
        this.searchApplicationName = searchApp.name();
        return new GetSearchApplicationAction.Response(searchApp);
    }

    @Override
    protected GetSearchApplicationAction.Response mutateInstance(GetSearchApplicationAction.Response instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected GetSearchApplicationAction.Response doParseInstance(XContentParser parser) throws IOException {
        return GetSearchApplicationAction.Response.fromXContent(this.searchApplicationName, parser);
    }

    @Override
    protected GetSearchApplicationAction.Response mutateInstanceForVersion(
        GetSearchApplicationAction.Response instance,
        TransportVersion version
    ) {
        return new GetSearchApplicationAction.Response(instance.searchApp());
    }
}
