/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction;
import org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response.FollowerInfo;
import org.elasticsearch.xpack.core.ccr.action.FollowParameters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response.FOLLOWER_INDICES_FIELD;
import static org.elasticsearch.xpack.core.ccr.action.FollowInfoAction.Response.Status;

public class FollowInfoResponseTests extends AbstractSerializingTestCase<FollowInfoAction.Response> {

    static final ConstructingObjectParser<FollowerInfo, Void> INFO_PARSER = new ConstructingObjectParser<>("info_parser", args -> {
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
        INFO_PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            FollowParametersTests.PARSER,
            FollowerInfo.PARAMETERS_FIELD
        );
    }

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<FollowInfoAction.Response, Void> PARSER = new ConstructingObjectParser<>(
        "response",
        args -> { return new FollowInfoAction.Response((List<FollowerInfo>) args[0]); }
    );

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
                followParameters = FollowParametersTests.randomInstance();
            }

            infos.add(
                new FollowerInfo(
                    randomAlphaOfLength(4),
                    randomAlphaOfLength(4),
                    randomAlphaOfLength(4),
                    randomFrom(Status.values()),
                    followParameters
                )
            );
        }
        return new FollowInfoAction.Response(infos);
    }
}
