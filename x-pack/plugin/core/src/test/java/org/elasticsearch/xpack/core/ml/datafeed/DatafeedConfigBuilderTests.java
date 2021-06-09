/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Collections;

import static org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfigTests.createRandomizedDatafeedConfigBuilder;

public class DatafeedConfigBuilderTests extends AbstractWireSerializingTestCase<DatafeedConfig.Builder> {

    @Override
    protected DatafeedConfig.Builder createTestInstance() {
        return createRandomizedDatafeedConfigBuilder(
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(10),
            3600000
        );
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected Writeable.Reader<DatafeedConfig.Builder> instanceReader() {
        return DatafeedConfig.Builder::new;
    }

}
