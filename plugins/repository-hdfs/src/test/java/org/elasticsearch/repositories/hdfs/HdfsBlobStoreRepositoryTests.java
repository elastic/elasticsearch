/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.hdfs;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.ESBlobStoreRepositoryIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;

@ThreadLeakFilters(filters = HdfsClientThreadLeakFilter.class)
// Ony using a single node here since the TestingFs only supports the single-node case
@ESIntegTestCase.ClusterScope(numDataNodes = 1, supportsDedicatedMasters = false)
public class HdfsBlobStoreRepositoryTests extends ESBlobStoreRepositoryIntegTestCase {

    @Override
    protected String repositoryType() {
        return "hdfs";
    }

    @Override
    protected Settings repositorySettings(String repoName) {
        return Settings.builder()
            .put("uri", "hdfs:///")
            .put("conf.fs.AbstractFileSystem.hdfs.impl", TestingFs.class.getName())
            .put("path", "foo")
            .put("chunk_size", randomIntBetween(100, 1000) + "k")
            .put("compress", randomBoolean())
            .build();
    }

    @Override
    public void testSnapshotAndRestore() throws Exception {
        // the HDFS mockup doesn't preserve the repository contents after removing the repository
        testSnapshotAndRestore(false);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(HdfsPlugin.class);
    }
}
