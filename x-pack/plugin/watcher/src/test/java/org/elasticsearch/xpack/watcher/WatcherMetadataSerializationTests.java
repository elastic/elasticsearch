/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.watcher.WatcherMetadata;

import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class WatcherMetadataSerializationTests extends ESTestCase {
    public void testXContentSerializationOneSignedWatcher() throws Exception {
        boolean manuallyStopped = randomBoolean();
        WatcherMetadata watcherMetadata = new WatcherMetadata(manuallyStopped);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject("watcher");
        watcherMetadata.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        builder.endObject();
        WatcherMetadata watchersMetadataFromXContent = getWatcherMetadataFromXContent(createParser(builder));
        assertThat(watchersMetadataFromXContent.manuallyStopped(), equalTo(manuallyStopped));
    }

    public void testWatcherMetadataParsingDoesNotSwallowOtherMetadata() throws Exception {
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        new Watcher(settings);  // makes sure WatcherMetadata is registered in Custom Metadata
        boolean manuallyStopped = randomBoolean();
        WatcherMetadata watcherMetadata = new WatcherMetadata(manuallyStopped);
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata("repo", "fs", Settings.EMPTY);
        RepositoriesMetadata repositoriesMetadata = new RepositoriesMetadata(Collections.singletonList(repositoryMetadata));
        final Metadata.Builder metadataBuilder = Metadata.builder();
        if (randomBoolean()) { // random order of insertion
            metadataBuilder.putCustom(watcherMetadata.getWriteableName(), watcherMetadata);
            metadataBuilder.putCustom(repositoriesMetadata.getWriteableName(), repositoriesMetadata);
        } else {
            metadataBuilder.putCustom(repositoriesMetadata.getWriteableName(), repositoriesMetadata);
            metadataBuilder.putCustom(watcherMetadata.getWriteableName(), watcherMetadata);
        }
        // serialize metadata
        XContentBuilder builder = XContentFactory.jsonBuilder();
        ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap(Metadata.CONTEXT_MODE_PARAM,
                Metadata.CONTEXT_MODE_GATEWAY));
        builder.startObject();
        builder = metadataBuilder.build().toXContent(builder, params);
        builder.endObject();
        // deserialize metadata again
        Metadata metadata = Metadata.Builder.fromXContent(createParser(builder));
        // check that custom metadata still present
        assertThat(metadata.custom(watcherMetadata.getWriteableName()), notNullValue());
        assertThat(metadata.custom(repositoriesMetadata.getWriteableName()), notNullValue());
    }

    private static WatcherMetadata getWatcherMetadataFromXContent(XContentParser parser) throws Exception {
        parser.nextToken(); // consume null
        parser.nextToken(); // consume "watcher"
        WatcherMetadata watcherMetadataFromXContent = (WatcherMetadata)WatcherMetadata.fromXContent(parser);
        parser.nextToken(); // consume endObject
        assertThat(parser.nextToken(), nullValue());
        return watcherMetadataFromXContent;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(Stream.concat(
                new XPackClientPlugin(Settings.builder().put("path.home", createTempDir()).build()).getNamedXContent().stream(),
                ClusterModule.getNamedXWriteables().stream()
        ).collect(Collectors.toList()));
    }

}
