/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.test.ESTestCase;

public class RemoteClusterAwareTests extends ESTestCase {

    public void testBuildRemoteIndexName() {
        {
            String clusterAlias = randomAlphaOfLengthBetween(5, 10);
            String index = randomAlphaOfLengthBetween(5, 10);
            String remoteIndexName = RemoteClusterAware.buildRemoteIndexName(clusterAlias, index);
            assertEquals(clusterAlias + ":" + index, remoteIndexName);
        }
        {
            String index = randomAlphaOfLengthBetween(5, 10);
            String clusterAlias = randomBoolean() ? null : RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
            String remoteIndexName = RemoteClusterAware.buildRemoteIndexName(clusterAlias, index);
            assertEquals(index, remoteIndexName);
        }
    }
}
