/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.rollup;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class GetRollupIndexCapsRequestTests extends ESTestCase {

    public void testNullOrEmptyIndices() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GetRollupIndexCapsRequest((String[]) null));
        assertThat(e.getMessage(), equalTo("[indices] must not be null or empty"));

        String[] indices = new String[]{};
        e = expectThrows(IllegalArgumentException.class, () -> new GetRollupIndexCapsRequest(indices));
        assertThat(e.getMessage(), equalTo("[indices] must not be null or empty"));

        e = expectThrows(IllegalArgumentException.class, () -> new GetRollupIndexCapsRequest(new String[]{"foo", null}));
        assertThat(e.getMessage(), equalTo("[index] must not be null or empty"));
    }
}
