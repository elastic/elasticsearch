/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

public class BulkRequestBuilderTests extends ESTestCase {

    public void testValidation() {
        BulkRequestBuilder bulkRequestBuilder = new BulkRequestBuilder(null, null);
        bulkRequestBuilder.add(new IndexRequestBuilder(null, randomAlphaOfLength(10)));
        BulkRequest bulkRequest1 = bulkRequestBuilder.request();
        assertNotNull(bulkRequest1);
        BulkRequest bulkRequest2 = bulkRequestBuilder.request();
        assertNotEquals(bulkRequest1, bulkRequest2);
        bulkRequestBuilder.add(new IndexRequest());
        expectThrows(IllegalStateException.class, bulkRequestBuilder::request);

        bulkRequestBuilder = new BulkRequestBuilder(null, null);
        bulkRequestBuilder.add(new IndexRequestBuilder(null, randomAlphaOfLength(10)));
        bulkRequestBuilder.setTimeout(randomTimeValue());
        assertNotNull(bulkRequestBuilder.request());
        bulkRequestBuilder.setTimeout(TimeValue.timeValueSeconds(randomIntBetween(1, 30)));
        expectThrows(IllegalStateException.class, bulkRequestBuilder::request);

        bulkRequestBuilder = new BulkRequestBuilder(null, null);
        bulkRequestBuilder.add(new IndexRequestBuilder(null, randomAlphaOfLength(10)));
        bulkRequestBuilder.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()).getValue());
        assertNotNull(bulkRequestBuilder.request());
        bulkRequestBuilder.setRefreshPolicy(randomFrom(WriteRequest.RefreshPolicy.values()));
        expectThrows(IllegalStateException.class, bulkRequestBuilder::request);
    }
}
