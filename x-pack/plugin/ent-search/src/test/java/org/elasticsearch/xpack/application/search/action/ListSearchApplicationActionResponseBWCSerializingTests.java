/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.application.EnterpriseSearchModuleTestUtils;
import org.elasticsearch.xpack.application.search.SearchApplication;
import org.elasticsearch.xpack.application.search.SearchApplicationListItem;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.util.List;

public class ListSearchApplicationActionResponseBWCSerializingTests extends AbstractBWCWireSerializationTestCase<
    ListSearchApplicationAction.Response> {

    @Override
    protected Writeable.Reader<ListSearchApplicationAction.Response> instanceReader() {
        return ListSearchApplicationAction.Response::new;
    }

    private static List<SearchApplicationListItem> randomSearchApplicationList() {
        return randomList(10, () -> {
            SearchApplication app = EnterpriseSearchModuleTestUtils.randomSearchApplication();
            return new SearchApplicationListItem(app.name(), app.analyticsCollectionName(), app.updatedAtMillis());
        });
    }

    @Override
    protected ListSearchApplicationAction.Response mutateInstance(ListSearchApplicationAction.Response instance) {
        List<SearchApplicationListItem> searchApplicationListItems = instance.queryPage.results();
        long count = instance.queryPage.count();
        switch (randomIntBetween(0, 1)) {
            case 0 -> searchApplicationListItems = randomValueOtherThan(searchApplicationListItems, () -> randomSearchApplicationList());
            case 1 -> count = randomValueOtherThan(count, () -> randomLongBetween(0, 1000));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new ListSearchApplicationAction.Response(searchApplicationListItems, count);
    }

    @Override
    protected ListSearchApplicationAction.Response createTestInstance() {
        return new ListSearchApplicationAction.Response(randomSearchApplicationList(), randomLongBetween(0, 1000));
    }

    @Override
    protected ListSearchApplicationAction.Response mutateInstanceForVersion(
        ListSearchApplicationAction.Response instance,
        TransportVersion version
    ) {
        return instance;
    }
}
