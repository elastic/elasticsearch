/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.support.AbstractSerializingTestCase;

public class DatafeedTests extends AbstractSerializingTestCase<Datafeed> {

    @Override
    protected Datafeed createTestInstance() {
        return new Datafeed(DatafeedConfigTests.createRandomizedDatafeedConfig(randomAsciiOfLength(10)),
                randomFrom(DatafeedStatus.values()));
    }

    @Override
    protected Writeable.Reader<Datafeed> instanceReader() {
        return Datafeed::new;
    }

    @Override
    protected Datafeed parseInstance(XContentParser parser) {
        return Datafeed.PARSER.apply(parser, null);
    }
}
