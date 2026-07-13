/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.job.snapshot.upgrade;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class SnapshotUpgradeTaskParamsTests extends AbstractXContentSerializingTestCase<SnapshotUpgradeTaskParams> {

    @Override
    protected SnapshotUpgradeTaskParams doParseInstance(XContentParser parser) throws IOException {
        return SnapshotUpgradeTaskParams.fromXContent(parser);
    }

    @Override
    protected SnapshotUpgradeTaskParams createTestInstance() {
        return new SnapshotUpgradeTaskParams(randomAlphaOfLength(10), randomAlphaOfLength(20));
    }

    @Override
    protected SnapshotUpgradeTaskParams mutateInstance(SnapshotUpgradeTaskParams instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<SnapshotUpgradeTaskParams> instanceReader() {
        return SnapshotUpgradeTaskParams::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
