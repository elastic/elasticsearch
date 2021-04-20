/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.forcemerge;

import org.elasticsearch.test.ESTestCase;

public class ForceMergeRequestTests extends ESTestCase {

    public void testDescription() {
        ForceMergeRequest request = new ForceMergeRequest();
        assertEquals("Force-merge indices [], maxSegments[-1], onlyExpungeDeletes[false], flush[true]", request.getDescription());

        request = new ForceMergeRequest("shop", "blog");
        assertEquals("Force-merge indices [shop, blog], maxSegments[-1], onlyExpungeDeletes[false], flush[true]", request.getDescription());

        request = new ForceMergeRequest();
        request.maxNumSegments(12);
        request.onlyExpungeDeletes(true);
        request.flush(false);
        assertEquals("Force-merge indices [], maxSegments[12], onlyExpungeDeletes[true], flush[false]", request.getDescription());
    }
}
