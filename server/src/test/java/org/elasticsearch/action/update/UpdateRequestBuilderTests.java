/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.update;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

public class UpdateRequestBuilderTests extends ESTestCase {

    public void testValidation() {
        UpdateRequestBuilder updateRequestBuilder = new UpdateRequestBuilder(null);
        updateRequestBuilder.setFetchSource(randomAlphaOfLength(10), randomAlphaOfLength(10));
        updateRequestBuilder.setFetchSource(true);
        expectThrows(IllegalStateException.class, updateRequestBuilder::request);

        updateRequestBuilder = new UpdateRequestBuilder(null);
        updateRequestBuilder.setTimeout(randomTimeValue());
        updateRequestBuilder.setTimeout(TimeValue.timeValueSeconds(randomIntBetween(1, 30)));
        expectThrows(IllegalStateException.class, updateRequestBuilder::request);

        updateRequestBuilder = new UpdateRequestBuilder(null);
        updateRequestBuilder.setDoc("key", "value");
        updateRequestBuilder.setDoc(Map.of("key", "value"));
        expectThrows(IllegalStateException.class, updateRequestBuilder::request);

        updateRequestBuilder = new UpdateRequestBuilder(null);
        updateRequestBuilder.setUpsert("key", "value");
        updateRequestBuilder.setUpsert(Map.of("key", "value"));
        expectThrows(IllegalStateException.class, updateRequestBuilder::request);
    }
}
