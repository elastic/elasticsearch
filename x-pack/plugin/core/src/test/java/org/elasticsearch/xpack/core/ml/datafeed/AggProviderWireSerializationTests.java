/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.utils.XContentObjectTransformer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class AggProviderWireSerializationTests extends AbstractBWCWireSerializationTestCase<AggProvider> {

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return writableRegistry();
    }

    @Override
    protected AggProvider createTestInstance() {
        return createRandomValidAggProvider();
    }

    @Override
    protected Writeable.Reader<AggProvider> instanceReader() {
        return AggProvider::fromStream;
    }

    public static AggProvider createRandomValidAggProvider() {
        Map<String, Object> agg = Collections.singletonMap(randomAlphaOfLengthBetween(1, 10),
            Collections.singletonMap("avg", Collections.singletonMap("field", randomAlphaOfLengthBetween(1, 10))));
        try {
            SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
            AggregatorFactories.Builder aggs =
                XContentObjectTransformer.aggregatorTransformer(new NamedXContentRegistry(searchModule.getNamedXContents()))
                    .fromMap(agg);
            Exception parsingException = null;
            if (randomBoolean()) {
                aggs = null;
                parsingException = new ElasticsearchException("bad configs");
            }
            return new AggProvider(agg, aggs, parsingException, randomBoolean());
        } catch (IOException ex) {
            fail(ex.getMessage());
        }
        return null;
    }

    @Override
    protected AggProvider mutateInstanceForVersion(AggProvider instance, Version version) {
        if (version.onOrBefore(Version.V_8_0_0)) {
            return new AggProvider(instance.getAggs(), instance.getParsedAggs(), instance.getParsingException(), false);
        }
        return instance;
    }
}
