/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxSizeCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class ElasticsearchNodeCommandTests extends ESTestCase {

    public void testLoadStateWithoutMissingCustoms() throws IOException {
        runLoadStateTest(false, false);
    }

    public void testLoadStateWithoutMissingCustomsButPreserved() throws IOException {
        runLoadStateTest(false, true);
    }

    public void testLoadStateWithMissingCustomsButPreserved() throws IOException {
        runLoadStateTest(true, true);
    }

    public void testLoadStateWithMissingCustomsAndNotPreserved() throws IOException {
        runLoadStateTest(true, false);
    }

    private void runLoadStateTest(boolean hasMissingCustoms, boolean preserveUnknownCustoms) throws IOException {
        final MetaData latestMetaData = randomMeta();
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        latestMetaData.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        MetaData loadedMetaData;
        try (XContentParser parser = createParser(hasMissingCustoms ? ElasticsearchNodeCommand.namedXContentRegistry : xContentRegistry(),
            JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            loadedMetaData = MetaData.fromXContent(parser);
        }
        assertThat(loadedMetaData.clusterUUID(), not(equalTo("_na_")));
        assertThat(loadedMetaData.clusterUUID(), equalTo(latestMetaData.clusterUUID()));
        ImmutableOpenMap<String, IndexMetaData> indices = loadedMetaData.indices();
        assertThat(indices.size(), equalTo(latestMetaData.indices().size()));
        for (IndexMetaData original : latestMetaData) {
            IndexMetaData deserialized = indices.get(original.getIndex().getName());
            assertThat(deserialized, notNullValue());
            assertThat(deserialized.getVersion(), equalTo(original.getVersion()));
            assertThat(deserialized.getMappingVersion(), equalTo(original.getMappingVersion()));
            assertThat(deserialized.getSettingsVersion(), equalTo(original.getSettingsVersion()));
            assertThat(deserialized.getNumberOfReplicas(), equalTo(original.getNumberOfReplicas()));
            assertThat(deserialized.getNumberOfShards(), equalTo(original.getNumberOfShards()));
        }

        // make sure the index tombstones are the same too
        if (hasMissingCustoms) {
            assertNotNull(loadedMetaData.custom(IndexGraveyard.TYPE));
            assertThat(loadedMetaData.custom(IndexGraveyard.TYPE), instanceOf(ElasticsearchNodeCommand.UnknownMetaDataCustom.class));

            if (preserveUnknownCustoms) {
                // check that we reserialize unknown metadata correctly again
                final Path tempdir = createTempDir();
                MetaData.FORMAT.write(loadedMetaData, tempdir);
                final MetaData reloadedMetaData = MetaData.FORMAT.loadLatestState(logger, xContentRegistry(), tempdir);
                assertThat(reloadedMetaData.indexGraveyard(), equalTo(latestMetaData.indexGraveyard()));
            }
        }  else {
            assertThat(loadedMetaData.indexGraveyard(), equalTo(latestMetaData.indexGraveyard()));
        }
    }

    private MetaData randomMeta() {
        int numIndices = randomIntBetween(1, 10);
        MetaData.Builder mdBuilder = MetaData.builder();
        mdBuilder.generateClusterUuidIfNeeded();
        for (int i = 0; i < numIndices; i++) {
            mdBuilder.put(indexBuilder(randomAlphaOfLength(10) + "idx-"+i));
        }
        int numDelIndices = randomIntBetween(0, 5);
        final IndexGraveyard.Builder graveyard = IndexGraveyard.builder();
        for (int i = 0; i < numDelIndices; i++) {
            graveyard.addTombstone(new Index(randomAlphaOfLength(10) + "del-idx-" + i, UUIDs.randomBase64UUID()));
        }
        mdBuilder.indexGraveyard(graveyard.build());
        return mdBuilder.build();
    }

    private IndexMetaData.Builder indexBuilder(String index) {
        return IndexMetaData.builder(index)
            .settings(settings(Version.CURRENT).put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 10))
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(0, 5)))
            .putRolloverInfo(new RolloverInfo("test", randomSubsetOf(Arrays.asList(
                new MaxAgeCondition(TimeValue.timeValueSeconds(100)),
                new MaxDocsCondition(100L),
                new MaxSizeCondition(new ByteSizeValue(100))
            )), 0));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(Stream.of(ClusterModule.getNamedXWriteables().stream(), IndicesModule.getNamedXContents().stream())
            .flatMap(Function.identity())
            .collect(Collectors.toList()));
    }
}
