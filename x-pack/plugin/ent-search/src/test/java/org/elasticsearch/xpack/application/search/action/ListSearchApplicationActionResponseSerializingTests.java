/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.application.search.SearchApplication;
import org.elasticsearch.xpack.application.search.SearchApplicationListItem;
import org.elasticsearch.xpack.application.search.SearchApplicationTestUtils;

public class ListSearchApplicationActionResponseSerializingTests extends AbstractWireSerializingTestCase<
    ListSearchApplicationAction.Response> {

    @Override
    protected Writeable.Reader<ListSearchApplicationAction.Response> instanceReader() {
        return ListSearchApplicationAction.Response::new;
    }

    private static ListSearchApplicationAction.Response randomSearchApplicationListItem() {
        return new ListSearchApplicationAction.Response(randomList(10, () -> {
            SearchApplication app = SearchApplicationTestUtils.randomSearchApplication();
            return new SearchApplicationListItem(app.name(), app.indices(), app.analyticsCollectionName(), app.updatedAtMillis());
        }), randomLongBetween(0, 1000));
    }

    @Override
    protected ListSearchApplicationAction.Response mutateInstance(ListSearchApplicationAction.Response instance) {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    @Override
    protected ListSearchApplicationAction.Response createTestInstance() {
        return randomSearchApplicationListItem();
    }
}
