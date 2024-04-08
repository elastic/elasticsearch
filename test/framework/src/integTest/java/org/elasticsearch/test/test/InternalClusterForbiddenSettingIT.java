/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.test;

import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.index.IndexVersionUtils;

/**
 * This test ensures that after a cluster restart, the forbidPrivateIndexSettings value
 * is kept
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class InternalClusterForbiddenSettingIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testRestart() throws Exception {
        IndexVersion version = IndexVersionUtils.randomPreviousCompatibleVersion(random(), IndexVersion.current());
        // create / delete an index with forbidden setting
        prepareCreate("test").setSettings(settings(version).build()).get();
        indicesAdmin().prepareDelete("test").get();
        // full restart
        internalCluster().fullRestart();
        // create / delete an index with forbidden setting
        prepareCreate("test").setSettings(settings(version).build()).get();
        indicesAdmin().prepareDelete("test").get();
    }

    public void testRollingRestart() throws Exception {
        IndexVersion version = IndexVersionUtils.randomPreviousCompatibleVersion(random(), IndexVersion.current());
        // create / delete an index with forbidden setting
        prepareCreate("test").setSettings(settings(version).build()).get();
        indicesAdmin().prepareDelete("test").get();
        // rolling restart
        internalCluster().rollingRestart(new InternalTestCluster.RestartCallback());
        // create / delete an index with forbidden setting
        prepareCreate("test").setSettings(settings(version).build()).get();
        indicesAdmin().prepareDelete("test").get();
    }
}
