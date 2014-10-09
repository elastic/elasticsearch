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
package org.elasticsearch.index.merge.policy;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.distributor.LeastUsedDistributor;
import org.elasticsearch.index.store.ram.RamDirectoryService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.EMPTY_SETTINGS;
import static org.hamcrest.Matchers.equalTo;

public class MergePolicySettingsTest extends ElasticsearchTestCase {

    protected final ShardId shardId = new ShardId(new Index("index"), 1);

    @Test
    public void testCompoundFileSettings() throws IOException {
        IndexSettingsService service = new IndexSettingsService(new Index("test"), EMPTY_SETTINGS);

        assertThat(new TieredMergePolicyProvider(createStore(EMPTY_SETTINGS), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new TieredMergePolicyProvider(createStore(build(true)), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new TieredMergePolicyProvider(createStore(build(0.5)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.5));
        assertThat(new TieredMergePolicyProvider(createStore(build(1.0)), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new TieredMergePolicyProvider(createStore(build("true")), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new TieredMergePolicyProvider(createStore(build("True")), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new TieredMergePolicyProvider(createStore(build("False")), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new TieredMergePolicyProvider(createStore(build("false")), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new TieredMergePolicyProvider(createStore(build(false)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new TieredMergePolicyProvider(createStore(build(0)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new TieredMergePolicyProvider(createStore(build(0.0)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));

        assertThat(new LogByteSizeMergePolicyProvider(createStore(EMPTY_SETTINGS), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new LogByteSizeMergePolicyProvider(createStore(build(true)), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new LogByteSizeMergePolicyProvider(createStore(build(0.5)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.5));
        assertThat(new LogByteSizeMergePolicyProvider(createStore(build(1.0)), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new LogByteSizeMergePolicyProvider(createStore(build("true")), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new LogByteSizeMergePolicyProvider(createStore(build("True")), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new LogByteSizeMergePolicyProvider(createStore(build("False")), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new LogByteSizeMergePolicyProvider(createStore(build("false")), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new LogByteSizeMergePolicyProvider(createStore(build(false)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new LogByteSizeMergePolicyProvider(createStore(build(0)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new LogByteSizeMergePolicyProvider(createStore(build(0.0)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));

        assertThat(new LogDocMergePolicyProvider(createStore(EMPTY_SETTINGS), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new LogDocMergePolicyProvider(createStore(build(true)), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new LogDocMergePolicyProvider(createStore(build(0.5)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.5));
        assertThat(new LogDocMergePolicyProvider(createStore(build(1.0)), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new LogDocMergePolicyProvider(createStore(build("true")), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new LogDocMergePolicyProvider(createStore(build("True")), service).getMergePolicy().getNoCFSRatio(), equalTo(1.0));
        assertThat(new LogDocMergePolicyProvider(createStore(build("False")), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new LogDocMergePolicyProvider(createStore(build("false")), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new LogDocMergePolicyProvider(createStore(build(false)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new LogDocMergePolicyProvider(createStore(build(0)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        assertThat(new LogDocMergePolicyProvider(createStore(build(0.0)), service).getMergePolicy().getNoCFSRatio(), equalTo(0.0));

    }

    @Test
    public void testInvalidValue() throws IOException {
        IndexSettingsService service = new IndexSettingsService(new Index("test"), EMPTY_SETTINGS);
        try {
            new LogDocMergePolicyProvider(createStore(build(-0.1)), service).getMergePolicy().getNoCFSRatio();
            fail("exception expected");
        } catch (ElasticsearchIllegalArgumentException ex) {

        }
        try {
            new LogDocMergePolicyProvider(createStore(build(1.1)), service).getMergePolicy().getNoCFSRatio();
            fail("exception expected");
        } catch (ElasticsearchIllegalArgumentException ex) {

        }
        try {
            new LogDocMergePolicyProvider(createStore(build("Falsch")), service).getMergePolicy().getNoCFSRatio();
            fail("exception expected");
        } catch (ElasticsearchIllegalArgumentException ex) {

        }

    }

    @Test
    public void testUpdateSettings() throws IOException {
        {
            IndexSettingsService service = new IndexSettingsService(new Index("test"), EMPTY_SETTINGS);
            TieredMergePolicyProvider mp = new TieredMergePolicyProvider(createStore(EMPTY_SETTINGS), service);
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(0.0));

            service.refreshSettings(build(1.0));
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(1.0));

            service.refreshSettings(build(0.1));
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(0.1));

            service.refreshSettings(build(0.0));
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        }

        {
            IndexSettingsService service = new IndexSettingsService(new Index("test"), EMPTY_SETTINGS);
            LogByteSizeMergePolicyProvider mp = new LogByteSizeMergePolicyProvider(createStore(EMPTY_SETTINGS), service);
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(0.0));

            service.refreshSettings(build(1.0));
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(1.0));

            service.refreshSettings(build(0.1));
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(0.1));

            service.refreshSettings(build(0.0));
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        }

        {
            IndexSettingsService service = new IndexSettingsService(new Index("test"), EMPTY_SETTINGS);
            LogDocMergePolicyProvider mp = new LogDocMergePolicyProvider(createStore(EMPTY_SETTINGS), service);
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(0.0));

            service.refreshSettings(build(1.0));
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(1.0));

            service.refreshSettings(build(0.1));
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(0.1));

            service.refreshSettings(build(0.0));
            assertThat(mp.getMergePolicy().getNoCFSRatio(), equalTo(0.0));
        }
    }

    public Settings build(String value) {
        return ImmutableSettings.builder().put(AbstractMergePolicyProvider.INDEX_COMPOUND_FORMAT, value).build();
    }

    public Settings build(double value) {
        return ImmutableSettings.builder().put(AbstractMergePolicyProvider.INDEX_COMPOUND_FORMAT, value).build();
    }

    public Settings build(int value) {
        return ImmutableSettings.builder().put(AbstractMergePolicyProvider.INDEX_COMPOUND_FORMAT, value).build();
    }

    public Settings build(boolean value) {
        return ImmutableSettings.builder().put(AbstractMergePolicyProvider.INDEX_COMPOUND_FORMAT, value).build();
    }

    protected Store createStore(Settings settings) throws IOException {
        DirectoryService directoryService = new RamDirectoryService(shardId, EMPTY_SETTINGS);
        return new Store(shardId, settings, null, directoryService, new LeastUsedDistributor(directoryService));
    }

}
