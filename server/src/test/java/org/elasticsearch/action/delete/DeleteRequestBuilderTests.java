/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.delete;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

public class DeleteRequestBuilderTests extends ESTestCase {

    public void testValidation() {
        DeleteRequestBuilder deleteRequestBuilder = new DeleteRequestBuilder(null, randomAlphaOfLength(10));
        deleteRequestBuilder.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()).toString());
        deleteRequestBuilder.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        expectThrows(IllegalStateException.class, deleteRequestBuilder::request);

        deleteRequestBuilder = new DeleteRequestBuilder(null, randomAlphaOfLength(10));
        deleteRequestBuilder.setTimeout(randomTimeValue());
        deleteRequestBuilder.setTimeout(TimeValue.timeValueSeconds(randomIntBetween(1, 30)));
        expectThrows(IllegalStateException.class, deleteRequestBuilder::request);
    }
}
