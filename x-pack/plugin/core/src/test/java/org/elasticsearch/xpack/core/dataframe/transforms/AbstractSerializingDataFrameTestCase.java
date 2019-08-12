/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BaseAggregationBuilder;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.dataframe.DataFrameField;
import org.elasticsearch.xpack.core.dataframe.DataFrameNamedXContentProvider;
import org.junit.Before;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;

public abstract class AbstractSerializingDataFrameTestCase<T extends ToXContent & Writeable>
        extends AbstractSerializingTestCase<T> {

    protected static Params TO_XCONTENT_PARAMS = new ToXContent.MapParams(
            Collections.singletonMap(DataFrameField.FOR_INTERNAL_STORAGE, "true"));

    /**
     * Test case that ensures aggregation named objects are registered
     */
    private NamedWriteableRegistry namedWriteableRegistry;
    private NamedXContentRegistry namedXContentRegistry;

    @Before
    public void registerAggregationNamedObjects() throws Exception {
        // register aggregations as NamedWriteable
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());

        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteables.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, MockDeprecatedQueryBuilder.NAME,
                MockDeprecatedQueryBuilder::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(AggregationBuilder.class, MockDeprecatedAggregationBuilder.NAME,
                MockDeprecatedAggregationBuilder::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(SyncConfig.class, DataFrameField.TIME_BASED_SYNC.getPreferredName(),
                TimeSyncConfig::new));

        List<NamedXContentRegistry.Entry> namedXContents = searchModule.getNamedXContents();
        namedXContents.add(new NamedXContentRegistry.Entry(QueryBuilder.class,
                new ParseField(MockDeprecatedQueryBuilder.NAME), (p, c) -> MockDeprecatedQueryBuilder.fromXContent(p)));
        namedXContents.add(new NamedXContentRegistry.Entry(BaseAggregationBuilder.class,
                new ParseField(MockDeprecatedAggregationBuilder.NAME), (p, c) -> MockDeprecatedAggregationBuilder.fromXContent(p)));
        namedXContents.addAll(new DataFrameNamedXContentProvider().getNamedXContentParsers());

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
