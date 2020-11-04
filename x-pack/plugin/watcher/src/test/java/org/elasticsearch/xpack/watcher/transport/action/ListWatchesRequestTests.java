/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.watcher.transport.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.watcher.transport.actions.ListWatchesAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ListWatchesRequestTests extends AbstractSerializingTestCase<ListWatchesAction.Request> {

    @Override
    protected ListWatchesAction.Request doParseInstance(XContentParser parser) throws IOException {
        return ListWatchesAction.Request.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<ListWatchesAction.Request> instanceReader() {
        return ListWatchesAction.Request::new;
    }

    @Override
    protected ListWatchesAction.Request createTestInstance() {
        QueryBuilder query = null;
        if (randomBoolean()) {
            query = QueryBuilders.termQuery(randomAlphaOfLengthBetween(5, 20), randomAlphaOfLengthBetween(5, 20));
        }
        List<FieldSortBuilder> sorts = null;
        if (randomBoolean()) {
            int numSorts = randomIntBetween(1, 3);
            sorts = new ArrayList<>(numSorts);
            for (int i = 0; i < numSorts; i++) {
                sorts.add(SortBuilders.fieldSort(randomAlphaOfLengthBetween(5, 20)).order(randomFrom(SortOrder.values())));
            }
        }
        return new ListWatchesAction.Request(
            randomBoolean() ? randomIntBetween(0, 10000) : null,
            randomBoolean() ? randomIntBetween(0, 10000) : null,
            query,
            sorts
        );
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }
}
