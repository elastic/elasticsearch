/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xpack.core.ilm.WaitForFollowShardTasksStep.Info;
import org.elasticsearch.xpack.core.ilm.WaitForFollowShardTasksStep.Info.ShardFollowTaskInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class WaitForFollowShardTasksStepInfoTests extends AbstractXContentTestCase<Info> {

    private static final ConstructingObjectParser<ShardFollowTaskInfo, Void> SHARD_FOLLOW_TASK_INFO_PARSER =
        new ConstructingObjectParser<>(
            "shard_follow_task_info_parser",
            args -> new ShardFollowTaskInfo((String) args[0], (Integer) args[1], (Long) args[2], (Long) args[3])
        );

    static {
        SHARD_FOLLOW_TASK_INFO_PARSER.declareString(ConstructingObjectParser.constructorArg(), ShardFollowTaskInfo.FOLLOWER_INDEX_FIELD);
        SHARD_FOLLOW_TASK_INFO_PARSER.declareInt(ConstructingObjectParser.constructorArg(), ShardFollowTaskInfo.SHARD_ID_FIELD);
        SHARD_FOLLOW_TASK_INFO_PARSER.declareLong(ConstructingObjectParser.constructorArg(),
            ShardFollowTaskInfo.LEADER_GLOBAL_CHECKPOINT_FIELD);
        SHARD_FOLLOW_TASK_INFO_PARSER.declareLong(ConstructingObjectParser.constructorArg(),
            ShardFollowTaskInfo.FOLLOWER_GLOBAL_CHECKPOINT_FIELD);
    }

    private static final ConstructingObjectParser<Info, Void> INFO_PARSER = new ConstructingObjectParser<>(
        "info_parser",
        args -> {
            @SuppressWarnings("unchecked")
            Info info = new Info((List<ShardFollowTaskInfo>) args[0]);
            return info;
        }
    );

    static {
        INFO_PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), SHARD_FOLLOW_TASK_INFO_PARSER,
            Info.SHARD_FOLLOW_TASKS);
        INFO_PARSER.declareString((i, s) -> {}, Info.MESSAGE);
    }

    @Override
    protected Info createTestInstance() {
        int numInfos = randomIntBetween(0, 32);
        List<ShardFollowTaskInfo> shardFollowTaskInfos = new ArrayList<>(numInfos);
        for (int i = 0; i < numInfos; i++) {
            shardFollowTaskInfos.add(new ShardFollowTaskInfo(randomAlphaOfLength(3), randomIntBetween(0, 10),
                randomNonNegativeLong(), randomNonNegativeLong()));
        }
        return new Info(shardFollowTaskInfos);
    }

    @Override
    protected Info doParseInstance(XContentParser parser) throws IOException {
        return INFO_PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
