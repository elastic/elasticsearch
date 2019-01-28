/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.junit.Before;

import java.util.List;

import static java.util.Collections.emptyList;

public abstract class AbstractSerializingDataFrameTestCase<T extends ToXContent & Writeable>
        extends AbstractSerializingTestCase<T> {

    /**
     * Test case that ensures aggregation named objects are registered
     */
    private NamedWriteableRegistry namedWriteableRegistry;
    private NamedXContentRegistry namedXContentRegistry;

    @Before
    public void registerAggregationNamedObjects() throws Exception {
        // register aggregations as NamedWriteable
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList());

        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteables.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, MockDeprecatedQueryBuilder.NAME,
                MockDeprecatedQueryBuilder::new));

        List<NamedXContentRegistry.Entry> namedXContents = searchModule.getNamedXContents();
        namedXContents.add(new NamedXContentRegistry.Entry(QueryBuilder.class,
                new ParseField(MockDeprecatedQueryBuilder.NAME), (p, c) -> MockDeprecatedQueryBuilder.fromXContent(p)));

        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
        namedXContentRegistry = new NamedXContentRegistry(namedXContents);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return namedXContentRegistry;
    }
}
