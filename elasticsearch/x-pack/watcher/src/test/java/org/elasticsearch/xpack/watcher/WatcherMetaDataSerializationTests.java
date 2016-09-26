/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

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
        WatcherMetaData watchersMetaDataFromXContent = getWatcherMetaDataFromXContent(builder.bytes());
        assertThat(watchersMetaDataFromXContent.manuallyStopped(), equalTo(manuallyStopped));
    }

    public void testWatcherMetadataParsingDoesNotSwallowOtherMetaData() throws Exception {
        new Watcher(Settings.EMPTY);  // makes sure WatcherMetaData is registered in Custom MetaData
        boolean manuallyStopped = randomBoolean();
        WatcherMetaData watcherMetaData = new WatcherMetaData(manuallyStopped);
        RepositoryMetaData repositoryMetaData = new RepositoryMetaData("repo", "fs", Settings.EMPTY);
        RepositoriesMetaData repositoriesMetaData = new RepositoriesMetaData(repositoryMetaData);
        final MetaData.Builder metaDataBuilder = MetaData.builder();
        if (randomBoolean()) { // random order of insertion
            metaDataBuilder.putCustom(watcherMetaData.type(), watcherMetaData);
            metaDataBuilder.putCustom(repositoriesMetaData.type(), repositoriesMetaData);
        } else {
            metaDataBuilder.putCustom(repositoriesMetaData.type(), repositoriesMetaData);
            metaDataBuilder.putCustom(watcherMetaData.type(), watcherMetaData);
        }
        // serialize metadata
        XContentBuilder builder = XContentFactory.jsonBuilder();
        ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap(MetaData.CONTEXT_MODE_PARAM,
                MetaData.CONTEXT_MODE_GATEWAY));
        builder.startObject();
        builder = metaDataBuilder.build().toXContent(builder, params);
        builder.endObject();
        String serializedMetaData = builder.string();
        // deserialize metadata again
        MetaData metaData = MetaData.Builder.fromXContent(XContentFactory.xContent(XContentType.JSON).createParser(serializedMetaData));
        // check that custom metadata still present
        assertThat(metaData.custom(watcherMetaData.type()), notNullValue());
        assertThat(metaData.custom(repositoriesMetaData.type()), notNullValue());
    }

    private static WatcherMetaData getWatcherMetaDataFromXContent(BytesReference bytes) throws Exception {
        final XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(bytes);
        parser.nextToken(); // consume null
        parser.nextToken(); // consume "watcher"
        WatcherMetaData watcherMetaDataFromXContent = (WatcherMetaData)WatcherMetaData.PROTO.fromXContent(parser);
        parser.nextToken(); // consume endObject
        assertThat(parser.nextToken(), nullValue());
        return watcherMetaDataFromXContent;
    }

}
