/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.transport.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.searchafter.SearchAfterBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.watcher.transport.actions.QueryWatchesAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QueryWatchesRequestTests extends AbstractSerializingTestCase<QueryWatchesAction.Request> {

    @Override
    protected QueryWatchesAction.Request doParseInstance(XContentParser parser) throws IOException {
        return QueryWatchesAction.Request.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<QueryWatchesAction.Request> instanceReader() {
        return QueryWatchesAction.Request::new;
    }

    @Override
    protected QueryWatchesAction.Request createTestInstance() {
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
        SearchAfterBuilder searchAfter = null;
        if (randomBoolean()) {
            searchAfter = new SearchAfterBuilder();
            searchAfter.setSortValues(new Object[] { randomInt() });
        }
        return new QueryWatchesAction.Request(
            randomBoolean() ? randomIntBetween(0, 10000) : null,
            randomBoolean() ? randomIntBetween(0, 10000) : null,
            query,
            sorts,
            searchAfter
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
