/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
    protected Settings repositorySettings() {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/31498", HdfsRepositoryTests.isJava11());
        return Settings.builder()
            .put("uri", "hdfs:///")
            .put("conf.fs.AbstractFileSystem.hdfs.impl", TestingFs.class.getName())
            .put("path", "foo")
            .put("chunk_size", randomIntBetween(100, 1000) + "k")
            .put("compress", randomBoolean()).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(HdfsPlugin.class);
    }
}
