/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.delete;

import org.elasticsearch.test.ESTestCase;

public class DeleteIndexRequestTests extends ESTestCase {
    public void testGetDescription() {
        DeleteIndexRequest request = new DeleteIndexRequest("index0");
        assertEquals("indices[index0]", request.getDescription());
        request = request.indices("index1", "index2");
        assertEquals("indices[index1,index2]", request.getDescription());
    }
}
