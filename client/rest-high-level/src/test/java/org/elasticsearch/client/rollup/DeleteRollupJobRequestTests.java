/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.rollup;

import org.elasticsearch.test.ESTestCase;

public class DeleteRollupJobRequestTests extends ESTestCase {

    public void testRequireConfiguration() {
        final NullPointerException e = expectThrows(NullPointerException.class, ()-> new DeleteRollupJobRequest(null));
        assertEquals("id parameter must not be null", e.getMessage());
    }
}
