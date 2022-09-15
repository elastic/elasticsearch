/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.action.admin.cluster.node.shutdown.NodesRemovalPrevalidation.IsSafe;
import static org.elasticsearch.action.admin.cluster.node.shutdown.NodesRemovalPrevalidation.NodeResult;
import static org.elasticsearch.action.admin.cluster.node.shutdown.NodesRemovalPrevalidation.Result;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;

public class PrevalidateNodeRemovalResponseTests extends ESTestCase {

    public void testToXContent() throws IOException {
        PrevalidateNodeRemovalResponse simpleResp = new PrevalidateNodeRemovalResponse(
            new NodesRemovalPrevalidation(new Result(IsSafe.YES, ""), List.of())
        );
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.prettyPrint();
            simpleResp.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals("""
                {
                  "result" : {
                    "is_safe" : "YES",
                    "reason" : ""
                  },
                  "nodes" : [ ]
                }""", Strings.toString(builder));
        }

        PrevalidateNodeRemovalResponse respWithNodes = new PrevalidateNodeRemovalResponse(
            new NodesRemovalPrevalidation(
                new Result(IsSafe.UNKNOWN, ""),
                List.of(new NodeResult("node1", "id1", "externalId1", new Result(IsSafe.UNKNOWN, "node hosts a red shard copy")))
            )
        );
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.prettyPrint();
            respWithNodes.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals("""
                {
                  "result" : {
                    "is_safe" : "UNKNOWN",
                    "reason" : ""
                  },
                  "nodes" : [
                    {
                      "id" : "id1",
                      "name" : "node1",
                      "external_id" : "externalId1",
                      "result" : {
                        "is_safe" : "UNKNOWN",
                        "reason" : "node hosts a red shard copy"
                      }
                    }
                  ]
                }""", Strings.toString(builder));
        }
    }

    public void testSerialization() throws IOException {
        int noOfNodes = randomIntBetween(1, 10);
        List<NodeResult> nodes = new ArrayList<>(noOfNodes);
        Result result = createRandomResult();
        for (int i = 0; i < noOfNodes; i++) {
            nodes.add(new NodeResult(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10), createRandomResult()));
        }
        PrevalidateNodeRemovalResponse resp = new PrevalidateNodeRemovalResponse(new NodesRemovalPrevalidation(result, nodes));
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            resp.writeTo(output);
            try (StreamInput input = output.bytes().streamInput()) {
                PrevalidateNodeRemovalResponse deserialized = new PrevalidateNodeRemovalResponse(input);
                assertNotNull(deserialized.getPrevalidation());
                NodesRemovalPrevalidation prevalidation = deserialized.getPrevalidation();
                assertThat(prevalidation.getResult(), is(result));
                assertNotNull(prevalidation.getNodes());
                assertThat(prevalidation.getNodes(), equalTo(nodes));
            }
        }
    }

    private Result createRandomResult() {
        IsSafe isSafe = randomFrom(IsSafe.values());
        String reason = createRandomReason(isSafe);
        return new Result(isSafe, reason);
    }

    private String createRandomReason(IsSafe isSafe) {
        return isSafe == IsSafe.YES ? "" : randomAlphaOfLengthBetween(0, 1000);
    }
}
