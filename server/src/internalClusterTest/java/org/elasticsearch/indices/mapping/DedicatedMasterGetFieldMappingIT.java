/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.mapping;

import org.junit.Before;

import static org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import static org.elasticsearch.test.ESIntegTestCase.Scope;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class DedicatedMasterGetFieldMappingIT extends SimpleGetFieldMappingsIT {

    @Before
    public void before1() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startNode();
    }

}
