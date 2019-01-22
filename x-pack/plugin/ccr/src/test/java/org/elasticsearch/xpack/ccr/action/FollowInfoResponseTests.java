/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response.FollowerInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response.FOLLOWER_INDICES_FIELD;
import static org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response.FollowParameters;
import static org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response.Status;

public class FollowInfoResponseTests extends AbstractSerializingTestCase<FollowInfoAction.Response> {

    static final ConstructingObjectParser<FollowParameters, Void> PARAMETERS_PARSER = new ConstructingObjectParser<>(
        "parameters_parser",
        args -> {
            return new FollowParameters(
                (Integer) args[0],
                (ByteSizeValue) args[1],
                (Integer) args[2],
                (Integer) args[3],
                (ByteSizeValue) args[4],
                (Integer) args[5],
                (Integer) args[6],
                (ByteSizeValue) args[7],
                (TimeValue) args[8],
                (TimeValue) args[9]
            );
        });

    static {
        PARAMETERS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), ShardFollowTask.MAX_READ_REQUEST_OPERATION_COUNT);
        PARAMETERS_PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), ShardFollowTask.MAX_READ_REQUEST_SIZE.getPreferredName()),
            ShardFollowTask.MAX_READ_REQUEST_SIZE,
            ObjectParser.ValueType.STRING);
        PARAMETERS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), ShardFollowTask.MAX_OUTSTANDING_READ_REQUESTS);
        PARAMETERS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), ShardFollowTask.MAX_WRITE_REQUEST_OPERATION_COUNT);
        PARAMETERS_PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), ShardFollowTask.MAX_WRITE_REQUEST_SIZE.getPreferredName()),
            ShardFollowTask.MAX_WRITE_REQUEST_SIZE,
            ObjectParser.ValueType.STRING);
        PARAMETERS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), ShardFollowTask.MAX_OUTSTANDING_WRITE_REQUESTS);
        PARAMETERS_PARSER.declareInt(ConstructingObjectParser.constructorArg(), ShardFollowTask.MAX_WRITE_BUFFER_COUNT);
        PARAMETERS_PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), ShardFollowTask.MAX_WRITE_BUFFER_SIZE.getPreferredName()),
            ShardFollowTask.MAX_WRITE_BUFFER_SIZE,
            ObjectParser.ValueType.STRING);
        PARAMETERS_PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), ShardFollowTask.MAX_RETRY_DELAY.getPreferredName()),
            ShardFollowTask.MAX_RETRY_DELAY,
            ObjectParser.ValueType.STRING);
        PARAMETERS_PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> TimeValue.parseTimeValue(p.text(), ShardFollowTask.READ_POLL_TIMEOUT.getPreferredName()),
            ShardFollowTask.READ_POLL_TIMEOUT,
            ObjectParser.ValueType.STRING);
    }

    static final ConstructingObjectParser<FollowerInfo, Void> INFO_PARSER = new ConstructingObjectParser<>(
        "info_parser",
        args -> {
            return new FollowerInfo(
                (String) args[0],
                (String) args[1],
                (String) args[2],
                Status.fromString((String) args[3]),
                (FollowParameters) args[4]
            );
        });

    static {
        INFO_PARSER.declareString(ConstructingObjectParser.constructorArg(), FollowerInfo.FOLLOWER_INDEX_FIELD);
        INFO_PARSER.declareString(ConstructingObjectParser.constructorArg(), FollowerInfo.REMOTE_CLUSTER_FIELD);
        INFO_PARSER.declareString(ConstructingObjectParser.constructorArg(), FollowerInfo.LEADER_INDEX_FIELD);
        INFO_PARSER.declareString(ConstructingObjectParser.constructorArg(), FollowerInfo.STATUS_FIELD);
        INFO_PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), PARAMETERS_PARSER, FollowerInfo.PARAMETERS_FIELD);
    }

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<FollowInfoAction.Response, Void> PARSER = new ConstructingObjectParser<>(
        "response",
        args -> {
            return new FollowInfoAction.Response(
                (List<FollowerInfo>) args[0]
            );
        });

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), INFO_PARSER, FOLLOWER_INDICES_FIELD);
    }

    @Override
    protected FollowInfoAction.Response doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected Writeable.Reader<FollowInfoAction.Response> instanceReader() {
        return FollowInfoAction.Response::new;
    }

    @Override
    protected FollowInfoAction.Response createTestInstance() {
        int numInfos = randomIntBetween(0, 32);
        List<FollowerInfo> infos = new ArrayList<>(numInfos);
        for (int i = 0; i < numInfos; i++) {
            FollowParameters followParameters = null;
            if (randomBoolean()) {
                followParameters = new FollowParameters(
                    randomIntBetween(0, Integer.MAX_VALUE),
                    new ByteSizeValue(randomNonNegativeLong()),
                    randomIntBetween(0, Integer.MAX_VALUE),
                    randomIntBetween(0, Integer.MAX_VALUE),
                    new ByteSizeValue(randomNonNegativeLong()),
                    randomIntBetween(0, Integer.MAX_VALUE),
                    randomIntBetween(0, Integer.MAX_VALUE),
                    new ByteSizeValue(randomNonNegativeLong()),
                    new TimeValue(randomNonNegativeLong()),
                    new TimeValue(randomNonNegativeLong())
                );
            }

            infos.add(new FollowerInfo(randomAlphaOfLength(4), randomAlphaOfLength(4), randomAlphaOfLength(4),
                randomFrom(Status.values()), followParameters));
        }
        return new FollowInfoAction.Response(infos);
    }
}
