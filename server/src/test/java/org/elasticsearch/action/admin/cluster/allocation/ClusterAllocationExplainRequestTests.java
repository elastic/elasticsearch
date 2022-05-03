/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

public class ClusterAllocationExplainRequestTests extends ESTestCase {

    public void testSerialization() throws Exception {
        ClusterAllocationExplainRequest request = new ClusterAllocationExplainRequest(
            randomAlphaOfLength(4),
            randomIntBetween(0, Integer.MAX_VALUE),
            randomBoolean(),
            randomBoolean() ? randomAlphaOfLength(5) : null
        );
        request.includeYesDecisions(randomBoolean());
        request.includeDiskInfo(randomBoolean());
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);

        ClusterAllocationExplainRequest actual = new ClusterAllocationExplainRequest(output.bytes().streamInput());
        assertEquals(request.getIndex(), actual.getIndex());
        assertEquals(request.getShard(), actual.getShard());
        assertEquals(request.isPrimary(), actual.isPrimary());
        assertEquals(request.includeYesDecisions(), actual.includeYesDecisions());
        assertEquals(request.includeDiskInfo(), actual.includeDiskInfo());
        assertEquals(request.getCurrentNode(), actual.getCurrentNode());
    }

}
