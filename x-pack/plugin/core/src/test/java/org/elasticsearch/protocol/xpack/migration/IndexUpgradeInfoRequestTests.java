/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.protocol.xpack.migration;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.test.AbstractStreamableTestCase;

public class IndexUpgradeInfoRequestTests extends AbstractStreamableTestCase<IndexUpgradeInfoRequest> {
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


    public void testNullIndices() {
        expectThrows(NullPointerException.class, () -> new IndexUpgradeInfoRequest((String[])null));
        expectThrows(NullPointerException.class, () -> new IndexUpgradeInfoRequest().indices((String[])null));
    }

    @Override
    protected IndexUpgradeInfoRequest createBlankInstance() {
        return new IndexUpgradeInfoRequest();
    }
}
