/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createFirstBackingIndex;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
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

        XContentParserConfiguration parserConfig = hasMissingCustoms
            ? parserConfig().withRegistry(ElasticsearchNodeCommand.namedXContentRegistry)
            : parserConfig();
        Metadata loadedMetadata;
        try (XContentParser parser = createParser(parserConfig, JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            loadedMetadata = Metadata.fromXContent(parser);
        }
        assertThat(loadedMetadata.clusterUUID(), not(equalTo("_na_")));
        assertThat(loadedMetadata.clusterUUID(), equalTo(latestMetadata.clusterUUID()));
        assertThat(loadedMetadata.getProject().dataStreams(), equalTo(latestMetadata.getProject().dataStreams()));

        // make sure the index tombstones are the same too
        if (hasMissingCustoms) {
            assertNotNull(loadedMetadata.getProject().custom(IndexGraveyard.TYPE));
            assertThat(
                loadedMetadata.getProject().custom(IndexGraveyard.TYPE),
                instanceOf(ElasticsearchNodeCommand.UnknownProjectCustom.class)
            );

            if (preserveUnknownCustoms) {
                // check that we reserialize unknown metadata correctly again
                final Path tempdir = createTempDir();
                Metadata.FORMAT.write(loadedMetadata, tempdir);
                final Metadata reloadedMetadata = Metadata.FORMAT.loadLatestState(logger, xContentRegistry(), tempdir);
                assertThat(reloadedMetadata.getProject().indexGraveyard(), equalTo(latestMetadata.getProject().indexGraveyard()));
            }
        } else {
            assertThat(loadedMetadata.getProject().indexGraveyard(), equalTo(latestMetadata.getProject().indexGraveyard()));
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
        if (randomBoolean()) {
            int numDataStreams = randomIntBetween(0, 5);
            for (int i = 0; i < numDataStreams; i++) {
                String dataStreamName = "name" + 1;
                IndexMetadata backingIndex = createFirstBackingIndex(dataStreamName).build();
                mdBuilder.put(newInstance(dataStreamName, List.of(backingIndex.getIndex())));
            }
        }
        mdBuilder.indexGraveyard(graveyard.build());
        return mdBuilder.build();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            Stream.of(ClusterModule.getNamedXWriteables().stream(), IndicesModule.getNamedXContents().stream())
                .flatMap(Function.identity())
                .toList()
        );
    }
}
