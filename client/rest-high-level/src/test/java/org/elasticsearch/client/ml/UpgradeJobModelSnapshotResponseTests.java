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


public class UpgradeJobModelSnapshotResponseTests extends AbstractXContentTestCase<UpgradeJobModelSnapshotResponse> {

    @Override
    protected UpgradeJobModelSnapshotResponse createTestInstance() {
        return new UpgradeJobModelSnapshotResponse(randomBoolean() ? null : randomBoolean(),
            randomBoolean() ? null : randomAlphaOfLength(10));
    }

    @Override
    protected UpgradeJobModelSnapshotResponse doParseInstance(XContentParser parser) throws IOException {
        return UpgradeJobModelSnapshotResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
