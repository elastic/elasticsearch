/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class UpgradeJobModelSnapshotRequestTests extends AbstractXContentTestCase<UpgradeJobModelSnapshotRequest> {

    @Override
    protected UpgradeJobModelSnapshotRequest createTestInstance() {
        return new UpgradeJobModelSnapshotRequest(randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomTimeValue(),
            randomBoolean() ? null : randomBoolean());
    }

    @Override
    protected UpgradeJobModelSnapshotRequest doParseInstance(XContentParser parser) throws IOException {
        return UpgradeJobModelSnapshotRequest.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
