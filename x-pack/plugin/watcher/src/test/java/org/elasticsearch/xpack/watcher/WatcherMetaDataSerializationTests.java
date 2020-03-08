/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.watcher.WatcherMetaData;

import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class WatcherMetaDataSerializationTests extends ESTestCase {
    public void testXContentSerializationOneSignedWatcher() throws Exception {
        boolean manuallyStopped = randomBoolean();
        WatcherMetaData watcherMetaData = new WatcherMetaData(manuallyStopped);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject("watcher");
        watcherMetaData.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        builder.endObject();
        WatcherMetaData watchersMetaDataFromXContent = getWatcherMetaDataFromXContent(createParser(builder));
        assertThat(watchersMetaDataFromXContent.manuallyStopped(), equalTo(manuallyStopped));
    }

    public void testWatcherMetadataParsingDoesNotSwallowOtherMetaData() throws Exception {
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        new Watcher(settings);  // makes sure WatcherMetaData is registered in Custom MetaData
        boolean manuallyStopped = randomBoolean();
        WatcherMetaData watcherMetaData = new WatcherMetaData(manuallyStopped);
        RepositoryMetaData repositoryMetaData = new RepositoryMetaData("repo", "fs", Settings.EMPTY);
        RepositoriesMetaData repositoriesMetaData = new RepositoriesMetaData(Collections.singletonList(repositoryMetaData));
        final MetaData.Builder metaDataBuilder = MetaData.builder();
        if (randomBoolean()) { // random order of insertion
            metaDataBuilder.putCustom(watcherMetaData.getWriteableName(), watcherMetaData);
            metaDataBuilder.putCustom(repositoriesMetaData.getWriteableName(), repositoriesMetaData);
        } else {
            metaDataBuilder.putCustom(repositoriesMetaData.getWriteableName(), repositoriesMetaData);
            metaDataBuilder.putCustom(watcherMetaData.getWriteableName(), watcherMetaData);
        }
        // serialize metadata
        XContentBuilder builder = XContentFactory.jsonBuilder();
        ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap(MetaData.CONTEXT_MODE_PARAM,
                MetaData.CONTEXT_MODE_GATEWAY));
        builder.startObject();
        builder = metaDataBuilder.build().toXContent(builder, params);
        builder.endObject();
        // deserialize metadata again
        MetaData metaData = MetaData.Builder.fromXContent(createParser(builder), randomBoolean());
        // check that custom metadata still present
        assertThat(metaData.custom(watcherMetaData.getWriteableName()), notNullValue());
        assertThat(metaData.custom(repositoriesMetaData.getWriteableName()), notNullValue());
    }

    private static WatcherMetaData getWatcherMetaDataFromXContent(XContentParser parser) throws Exception {
        parser.nextToken(); // consume null
        parser.nextToken(); // consume "watcher"
        WatcherMetaData watcherMetaDataFromXContent = (WatcherMetaData)WatcherMetaData.fromXContent(parser);
        parser.nextToken(); // consume endObject
        assertThat(parser.nextToken(), nullValue());
        return watcherMetaDataFromXContent;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(Stream.concat(
                new XPackClientPlugin(Settings.builder().put("path.home", createTempDir()).build()).getNamedXContent().stream(),
                ClusterModule.getNamedXWriteables().stream()
        ).collect(Collectors.toList()));
    }

}
