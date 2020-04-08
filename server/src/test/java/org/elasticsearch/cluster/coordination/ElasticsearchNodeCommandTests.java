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
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

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
        final Metadata latestMetadata = randomMeta();
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        Metadata.FORMAT.toXContent(builder, latestMetadata);
        builder.endObject();

        Metadata loadedMetadata;
        try (XContentParser parser = createParser(hasMissingCustoms ? ElasticsearchNodeCommand.namedXContentRegistry : xContentRegistry(),
            JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            loadedMetadata = Metadata.fromXContent(parser);
        }
        assertThat(loadedMetadata.clusterUUID(), not(equalTo("_na_")));
        assertThat(loadedMetadata.clusterUUID(), equalTo(latestMetadata.clusterUUID()));

        // make sure the index tombstones are the same too
        if (hasMissingCustoms) {
            assertNotNull(loadedMetadata.custom(CUSTOM_TYPE));
            assertThat(loadedMetadata.custom(CUSTOM_TYPE), instanceOf(ElasticsearchNodeCommand.UnknownMetadataCustom.class));

            if (preserveUnknownCustoms) {
                // check that we reserialize unknown metadata correctly again
                final Path tempdir = createTempDir();
                Metadata.FORMAT.write(loadedMetadata, tempdir);
                final Metadata reloadedMetadata = Metadata.FORMAT.loadLatestState(logger, xContentRegistry(), tempdir);
                assertThat(reloadedMetadata.custom(CUSTOM_TYPE), equalTo(latestMetadata.custom(CUSTOM_TYPE)));
            }
        }  else {
            assertThat(loadedMetadata.custom(CUSTOM_TYPE), equalTo(latestMetadata.custom(CUSTOM_TYPE)));
        }
    }

    private Metadata randomMeta() {
        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.generateClusterUuidIfNeeded();
        int numDelIndices = randomIntBetween(0, 5);
        final IndexGraveyard.Builder graveyard = IndexGraveyard.builder();
        for (int i = 0; i < numDelIndices; i++) {
            graveyard.addTombstone(new Index(randomAlphaOfLength(10) + "del-idx-" + i, UUIDs.randomBase64UUID()));
        }
        mdBuilder.putCustom(CUSTOM_TYPE, new CustomGraveyard(graveyard.build()));
        return mdBuilder.build();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        Stream<Stream<NamedXContentRegistry.Entry>> stream = Stream.of(
            ClusterModule.getNamedXWriteables().stream(),
            IndicesModule.getNamedXContents().stream(),
            List.of(new NamedXContentRegistry.Entry(Metadata.Custom.class, new ParseField(CUSTOM_TYPE),
                CustomGraveyard::fromXContent)).stream()
        );
        return new NamedXContentRegistry(stream.flatMap(Function.identity()).collect(Collectors.toList()));
    }

    public static final String CUSTOM_TYPE = "custom_tombstones";

    private static class CustomGraveyard implements Metadata.Custom {

        private final IndexGraveyard indexGraveyard;

        CustomGraveyard(IndexGraveyard indexGraveyard) {
            this.indexGraveyard = indexGraveyard;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return indexGraveyard.context();
        }

        @Override
        public Diff<Metadata.Custom> diff(Metadata.Custom previousState) {
            return indexGraveyard.diff(previousState);
        }

        @Override
        public String getWriteableName() {
            return CUSTOM_TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return indexGraveyard.getMinimalSupportedVersion();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            indexGraveyard.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return indexGraveyard.toXContent(builder, params);
        }

        public static CustomGraveyard fromXContent(final XContentParser parser) throws IOException {
            return new CustomGraveyard(IndexGraveyard.fromXContent(parser));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CustomGraveyard that = (CustomGraveyard) o;
            return indexGraveyard.equals(that.indexGraveyard);
        }

        @Override
        public int hashCode() {
            return Objects.hash(indexGraveyard);
        }
    }
}
