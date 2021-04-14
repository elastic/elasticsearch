/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.hdfs;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.elasticsearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.AbstractThirdPartyRepositoryTestCase;

import java.util.Collection;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ThreadLeakFilters(filters = HdfsClientThreadLeakFilter.class)
public class HdfsRepositoryTests extends AbstractThirdPartyRepositoryTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(HdfsPlugin.class);
    }

    @Override
    protected SecureSettings credentials() {
        return new MockSecureSettings();
    }

    @Override
    protected void createRepository(String repoName) {
        AcknowledgedResponse putRepositoryResponse = client().admin().cluster().preparePutRepository(repoName)
            .setType("hdfs")
            .setSettings(Settings.builder()
                .put("uri", "hdfs:///")
                .put("conf.fs.AbstractFileSystem.hdfs.impl", TestingFs.class.getName())
                .put("path", "foo")
                .put("chunk_size", randomIntBetween(100, 1000) + "k")
                .put("compress", randomBoolean())
            ).get();
        assertThat(putRepositoryResponse.isAcknowledged(), equalTo(true));
    }

    // HDFS repository doesn't have precise cleanup stats so we only check whether or not any blobs were removed
    @Override
    protected void assertCleanupResponse(CleanupRepositoryResponse response, long bytes, long blobs) {
        if (blobs > 0) {
            assertThat(response.result().blobs(), greaterThan(0L));
        } else {
            assertThat(response.result().blobs(), equalTo(0L));
        }
    }
}
