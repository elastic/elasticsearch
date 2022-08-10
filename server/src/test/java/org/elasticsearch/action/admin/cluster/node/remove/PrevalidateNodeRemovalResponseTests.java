/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.remove;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.admin.cluster.node.remove.NodesRemovalPrevalidation.IsSafe;
import static org.elasticsearch.action.admin.cluster.node.remove.NodesRemovalPrevalidation.Result;

public class PrevalidateNodeRemovalResponseTests extends ESTestCase {

    public void testToXContent() throws IOException {
        PrevalidateNodeRemovalResponse simpleResp = new PrevalidateNodeRemovalResponse(
            new NodesRemovalPrevalidation(new Result(IsSafe.YES, ""), Map.of())
        );
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.prettyPrint();
            simpleResp.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals("""
                {
                  "overall_result" : {
                    "is_safe" : "YES",
                    "reason" : ""
                  },
                  "per_node_result" : { }
                }""", Strings.toString(builder));
        }

        PrevalidateNodeRemovalResponse respWithNodes = new PrevalidateNodeRemovalResponse(
            new NodesRemovalPrevalidation(
                new Result(IsSafe.UNKNOWN, ""),
                Map.of("node1", new Result(IsSafe.UNKNOWN, "node hosts a red shard copy"))
            )
        );
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.prettyPrint();
            respWithNodes.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals("""
                {
                  "overall_result" : {
                    "is_safe" : "UNKNOWN",
                    "reason" : ""
                  },
                  "per_node_result" : {
                    "node1" : {
                      "is_safe" : "UNKNOWN",
                      "reason" : "node hosts a red shard copy"
                    }
                  }
                }""", Strings.toString(builder));
        }
    }
}
