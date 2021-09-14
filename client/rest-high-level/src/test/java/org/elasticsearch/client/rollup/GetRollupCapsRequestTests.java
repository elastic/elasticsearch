/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.rollup;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class GetRollupCapsRequestTests extends ESTestCase {

    public void testImplicitIndexPattern() {
        String pattern = randomFrom("", "*", Metadata.ALL, null);
        GetRollupCapsRequest request = new GetRollupCapsRequest(pattern);
        assertThat(request.getIndexPattern(), equalTo(Metadata.ALL));
    }
}
