/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsAction;

public class RestGetMappingsCancellationIT extends ClusterBlockRestCancellationTestCase {
    public void testGetMappingsCancellation() throws Exception {
        createIndex("test");
        ensureGreen("test");
        runTest("/test/_mappings", GetMappingsAction.NAME);
    }
}
