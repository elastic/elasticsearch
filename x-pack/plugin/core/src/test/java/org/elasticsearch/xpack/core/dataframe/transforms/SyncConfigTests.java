/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class SyncConfigTests extends AbstractSerializingTestCase<SyncConfig>{

    public static SyncConfig randomSyncConfig() {
        return new SyncConfig(TimeSyncConfigTests.randomTimeSyncConfig());
    }

    @Override
    protected SyncConfig doParseInstance(XContentParser parser) throws IOException {
        return SyncConfig.fromXContent(parser, false);
    }

    @Override
    protected SyncConfig createTestInstance() {
        return randomSyncConfig();
    }

    @Override
    protected Reader<SyncConfig> instanceReader() {
        return SyncConfig::new;
    }

}
