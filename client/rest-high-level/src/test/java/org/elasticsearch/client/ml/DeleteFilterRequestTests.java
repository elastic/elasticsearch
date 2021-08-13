/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.test.ESTestCase;

public class DeleteFilterRequestTests extends ESTestCase {

    public void test_WithNullFilter() {
        NullPointerException ex = expectThrows(NullPointerException.class, () -> new DeleteFilterRequest(null));
        assertEquals("[filter_id] is required", ex.getMessage());
    }

    public void test_instance() {
        String filterId = randomAlphaOfLengthBetween(2, 10);
        DeleteFilterRequest deleteFilterRequest = new DeleteFilterRequest(filterId);
        assertEquals(deleteFilterRequest.getId(), filterId);
    }
}
