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

import org.elasticsearch.xpack.core.ccr.action.FollowParameters;
import static org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response.Status;

public class FollowInfoResponseTests extends AbstractSerializingTestCase<FollowInfoAction.Response> {

    static final ObjectParser<FollowParameters, Void> PARAMETERS_PARSER = new ObjectParser<>("parameters_parser", FollowParameters::new);
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
        FollowParameters.initParser(PARAMETERS_PARSER);

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
                followParameters = new FollowParameters();
                followParameters.setMaxOutstandingReadRequests(randomIntBetween(0, Integer.MAX_VALUE));
                followParameters.setMaxOutstandingWriteRequests(randomIntBetween(0, Integer.MAX_VALUE));
                followParameters.setMaxReadRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
                followParameters.setMaxWriteRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
                followParameters.setMaxReadRequestSize(new ByteSizeValue(randomNonNegativeLong()));
                followParameters.setMaxWriteRequestSize(new ByteSizeValue(randomNonNegativeLong()));
                followParameters.setMaxWriteBufferCount(randomIntBetween(0, Integer.MAX_VALUE));
                followParameters.setMaxWriteBufferSize(new ByteSizeValue(randomNonNegativeLong()));
                followParameters.setMaxRetryDelay(new TimeValue(randomNonNegativeLong()));
                followParameters.setReadPollTimeout(new TimeValue(randomNonNegativeLong()));
            }

            infos.add(new FollowerInfo(randomAlphaOfLength(4), randomAlphaOfLength(4), randomAlphaOfLength(4),
                randomFrom(Status.values()), followParameters));
        }
        return new FollowInfoAction.Response(infos);
    }
}
