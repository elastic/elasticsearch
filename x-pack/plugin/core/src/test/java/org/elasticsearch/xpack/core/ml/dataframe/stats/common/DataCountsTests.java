/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats.common;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

public class DataCountsTests extends AbstractBWCSerializationTestCase<DataCounts> {

    private boolean lenient;

    @Before
    public void chooseLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected DataCounts mutateInstanceForVersion(DataCounts instance, TransportVersion version) {
        return instance;
    }

    @Override
    protected DataCounts doParseInstance(XContentParser parser) throws IOException {
        return lenient ? DataCounts.LENIENT_PARSER.apply(parser, null) : DataCounts.STRICT_PARSER.apply(parser, null);
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));
    }

    @Override
    protected Writeable.Reader<DataCounts> instanceReader() {
        return DataCounts::new;
    }

    @Override
    protected DataCounts createTestInstance() {
        return createRandom();
    }

    @Override
    protected DataCounts mutateInstance(DataCounts instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static DataCounts createRandom() {
        return new DataCounts(randomAlphaOfLength(10), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }
}
