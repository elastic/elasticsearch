/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade.actions;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.protocol.xpack.migration.IndexUpgradeInfoRequest;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class IndexUpgradeInfoActionRequestTests extends AbstractWireSerializingTestCase<IndexUpgradeInfoRequest> {
    @Override
    protected IndexUpgradeInfoRequest createTestInstance() {
        int indexCount = randomInt(4);
        String[] indices = new String[indexCount];
        for (int i = 0; i < indexCount; i++) {
            indices[i] = randomAlphaOfLength(10);
        }
        IndexUpgradeInfoRequest request = new IndexUpgradeInfoRequest(indices);
        if (randomBoolean()) {
            request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        }
        return request;
    }

    @Override
    protected Writeable.Reader<IndexUpgradeInfoRequest> instanceReader() {
        return IndexUpgradeInfoRequest::new;
    }
}
