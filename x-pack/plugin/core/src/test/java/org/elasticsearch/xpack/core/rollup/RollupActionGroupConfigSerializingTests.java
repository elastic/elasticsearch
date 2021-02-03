/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.core.rollup.ConfigTestHelpers.randomRollupActionGroupConfig;

public class RollupActionGroupConfigSerializingTests extends AbstractSerializingTestCase<RollupActionGroupConfig> {

    @Override
    protected RollupActionGroupConfig doParseInstance(final XContentParser parser) throws IOException {
        return RollupActionGroupConfig.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<RollupActionGroupConfig> instanceReader() {
        return RollupActionGroupConfig::new;
    }

    @Override
    protected RollupActionGroupConfig createTestInstance() {
        return randomRollupActionGroupConfig(random());
    }
}
