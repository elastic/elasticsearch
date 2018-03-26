/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test.rest;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class ESRestTestCaseTests extends ESTestCase {
    public void testAttachSniffedMetadataOnClientOk() {
        RestClient client = mock(RestClient.class);
        Node[] originalNodes = new Node[] {
            new Node(new HttpHost("1")),
            new Node(new HttpHost("2")),
            new Node(new HttpHost("3")),
        };
        List<Node> nodesWithMetadata = Arrays.asList(new Node[] {
            // This node matches exactly:
            new Node(new HttpHost("1"), emptyList(), randomAlphaOfLength(5), randomRoles()),
            // This node also matches exactly but has bound hosts which don't matter:
            new Node(new HttpHost("2"), Arrays.asList(new HttpHost("2"), new HttpHost("not2")),
                    randomAlphaOfLength(5), randomRoles()),
            // This node's host doesn't match but one of its published hosts does so
            // we return a modified version of it:
            new Node(new HttpHost("not3"), Arrays.asList(new HttpHost("not3"), new HttpHost("3")),
                    randomAlphaOfLength(5), randomRoles()),
            // This node isn't in the original list so it isn't added:
            new Node(new HttpHost("4"), emptyList(), randomAlphaOfLength(5), randomRoles()),
        });
        ESRestTestCase.attachSniffedMetadataOnClient(client, originalNodes, nodesWithMetadata);
        verify(client).setNodes(new Node[] {
            nodesWithMetadata.get(0),
            nodesWithMetadata.get(1),
            nodesWithMetadata.get(2).withBoundHostAsHost(new HttpHost("3")),
        });
    }

    public void testAttachSniffedMetadataOnClientNotEnoughNodes() {
        // Try a version of the call that should fail because it doesn't have all the results
        RestClient client = mock(RestClient.class);
        Node[] originalNodes = new Node[] {
            new Node(new HttpHost("1")),
            new Node(new HttpHost("2")),
        };
        List<Node> nodesWithMetadata = Arrays.asList(new Node[] {
            // This node matches exactly:
            new Node(new HttpHost("1"), emptyList(), "v", new Node.Roles(true, true, true)),
        });
        IllegalStateException e = expectThrows(IllegalStateException.class, () ->
                ESRestTestCase.attachSniffedMetadataOnClient(client, originalNodes, nodesWithMetadata));
        assertEquals(e.getMessage(), "Didn't sniff metadata for all nodes. Wanted metadata for "
                + "[http://1, http://2] but got [[host=http://1, bound=[], version=v, roles=mdi]]");
        verify(client, never()).setNodes(any(Node[].class));
    }

    private Node.Roles randomRoles() {
        return new Node.Roles(randomBoolean(), randomBoolean(), randomBoolean());
    }
}
