/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;

import java.io.IOException;

public class PutFollowActionRequestTests extends AbstractXContentSerializingTestCase<PutFollowAction.Request> {

    @Override
    protected Writeable.Reader<PutFollowAction.Request> instanceReader() {
        return PutFollowAction.Request::new;
    }

    @Override
    protected PutFollowAction.Request createTestInstance() {
        PutFollowAction.Request request = new PutFollowAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        request.setFollowerIndex(randomAlphaOfLength(4));
        request.waitForActiveShards(
            randomFrom(ActiveShardCount.DEFAULT, ActiveShardCount.NONE, ActiveShardCount.ONE, ActiveShardCount.ALL)
        );

        request.setRemoteCluster(randomAlphaOfLength(4));
        request.setLeaderIndex(randomAlphaOfLength(4));
        request.setSettings(
            Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), randomIntBetween(0, 4)).build()
        );
        ResumeFollowActionRequestTests.generateFollowParameters(request.getParameters());
        request.setDataStreamName(randomAlphaOfLength(4));
        return request;
    }

    @Override
    protected PutFollowAction.Request createXContextTestInstance(XContentType xContentType) {
        // follower index parameter and wait for active shards params are not part of the request body and
        // are provided in the url path. So these fields cannot be used for creating a test instance for xcontent testing.
        PutFollowAction.Request request = new PutFollowAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        request.setRemoteCluster(randomAlphaOfLength(4));
        request.setLeaderIndex(randomAlphaOfLength(4));
        request.setSettings(
            Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), randomIntBetween(0, 4)).build()
        );
        request.setFollowerIndex("followerIndex");
        ResumeFollowActionRequestTests.generateFollowParameters(request.getParameters());
        request.setDataStreamName(randomAlphaOfLength(4));
        return request;
    }

    @Override
    protected PutFollowAction.Request doParseInstance(XContentParser parser) throws IOException {
        PutFollowAction.Request request = PutFollowAction.Request.fromXContent(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, parser);
        request.waitForActiveShards(ActiveShardCount.DEFAULT);
        request.setFollowerIndex("followerIndex");
        return request;
    }

    @Override
    protected PutFollowAction.Request mutateInstance(PutFollowAction.Request instance) {
        PutFollowAction.Request request = new PutFollowAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
        request.setFollowerIndex(instance.getFollowerIndex());
        request.waitForActiveShards(instance.waitForActiveShards());
        request.setRemoteCluster(instance.getRemoteCluster());
        request.setLeaderIndex(instance.getLeaderIndex());
        request.setSettings(instance.getSettings());
        request.setParameters(instance.getParameters());
        request.setDataStreamName(instance.getDataStreamName());

        switch (randomIntBetween(0, 6)) {
            case 0 -> request.setFollowerIndex(randomAlphaOfLength(5));
            case 1 -> request.waitForActiveShards(new ActiveShardCount(randomIntBetween(3, 5)));
            case 2 -> request.setRemoteCluster(randomAlphaOfLength(5));
            case 3 -> request.setLeaderIndex(randomAlphaOfLength(5));
            case 4 -> request.setSettings(
                Settings.builder()
                    .put(
                        IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(),
                        randomValueOtherThan(
                            IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(request.getSettings()),
                            ESTestCase::randomInt
                        )
                    )
                    .build()
            );
            case 5 -> request.setParameters(FollowParametersTests.randomInstance());
            case 6 -> request.setDataStreamName(randomAlphaOfLength(5));
            default -> throw new AssertionError("failed branch");
        }
        return request;
    }

}
