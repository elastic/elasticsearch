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
package org.elasticsearch.backwards;

import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

public class IndexingIT extends ESRestTestCase {

    private ObjectPath objectPath(Response response) throws IOException {
        String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        String contentType = response.getHeader("Content-Type");
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(contentType);
        return ObjectPath.createFromXContent(xContentType.xContent(), body);
    }

    private void assertOK(Response response) {
        assertThat(response.getStatusLine().getStatusCode(), anyOf(equalTo(200), equalTo(201)));
    }

    private void createIndex(String name, Settings settings) throws IOException {
        assertOK(client().performRequest("PUT", name, Collections.emptyMap(),
            new StringEntity("{ \"settings\": " + Strings.toString(settings, true) + " }")));

    }

    public void testGlobalCheckpoints() throws Exception {
        Nodes nodes = buildNodeAndVersions();
        Settings.Builder settings = Settings.builder()
            .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), nodes.size() - 1);
        final boolean checkGlobalCheckpoints = nodes.getMaster().getVersion().onOrAfter(Version.V_6_0_0_alpha1_UNRELEASED);
        logger.info("master version is [{}], global checkpoints will be [{}]", nodes.getMaster().getVersion(),
            checkGlobalCheckpoints ? "checked" : "not be checked");
        if (checkGlobalCheckpoints) {
            settings.put(IndexSettings.INDEX_SEQ_NO_CHECKPOINT_SYNC_INTERVAL.getKey(), "100ms");
        }
        createIndex("test", settings.build());

        final int numDocs = randomInt(10);
        for (int i = 0; i < numDocs; i++) {
            assertOK(client().performRequest("PUT", "test/test/" + i, emptyMap(),
                new StringEntity("{\"test\": \"test_" + i + "\"}")));
        }
        assertBusy(() -> {
            try {
                Response response = client().performRequest("GET", "test/_stats", singletonMap("level", "shards"));
                List<Object> shardStats = objectPath(response).evaluate("indices.test.shards.0");
                for (Object shard : shardStats) {
                    final String nodeId = ObjectPath.evaluate(shard, "routing.node");
                    final Boolean primary = ObjectPath.evaluate(shard, "routing.primary");
                    final Node node = nodes.getSafe(nodeId);
                    if (node.getVersion().onOrAfter(Version.V_6_0_0_alpha1_UNRELEASED)) {
                        Integer maxSeqNo = ObjectPath.evaluate(shard, "seq_no.max");
                        Integer localCheckpoint = ObjectPath.evaluate(shard, "seq_no.local_checkpoint");
                        Integer globalCheckpoint = ObjectPath.evaluate(shard, "seq_no.global_checkpoint");
                        logger.info("stats for {}, primary [{}]: maxSeqNo [{}], localCheckpoint [{}], globalCheckpoint [{}]",
                            node, primary, maxSeqNo, localCheckpoint, globalCheckpoint);
                        assertThat("max_seq no on " + node + " is wrong", maxSeqNo, equalTo(numDocs));
                        assertThat("localCheckpoint no on " + node + " is wrong", localCheckpoint, equalTo(numDocs - 1));
                        if (checkGlobalCheckpoints) {
                            assertThat("globalCheckpoint no on " + node + " is wrong", globalCheckpoint, equalTo(numDocs - 1));
                        }
                    } else {
                        logger.info("skipping seq no test on {}", node);
                    }
                }
            } catch (IOException e) {
                throw new AssertionError("unexpected io error", e);
            }
        });
    }

    private Nodes buildNodeAndVersions() throws IOException {
        Response response = client().performRequest("GET", "_nodes");
        ObjectPath objectPath = objectPath(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
        Nodes nodes = new Nodes();
        for (String id : nodesAsMap.keySet()) {
            nodes.add(new Node(
                id,
                objectPath.evaluate("nodes." + id + ".name"),
                Version.fromString(objectPath.evaluate("nodes." + id + ".version"))));
        }
        response = client().performRequest("GET", "_cluster/state");
        nodes.setMasterNodeId(objectPath(response).evaluate("master_node"));
        return nodes;
    }

    final class Nodes extends HashMap<String, Node> {

        private String masterNodeId = null;

        public Node getMaster() {
            return get(masterNodeId);
        }

        public void setMasterNodeId(String id) {
            if (get(id) == null) {
                throw new IllegalArgumentException("node with id [" + id + "] not found. got:" + toString());
            }
            masterNodeId = id;
        }

        public void add(Node node) {
            put(node.getId(), node);
        }

        public Node getSafe(String id) {
            Node node = get(id);
            if (node == null) {
                throw new IllegalArgumentException("node with id [" + id + "] not found");
            }
            return node;
        }

        @Override
        public String toString() {
            return "Nodes{" +
                "masterNodeId='" + masterNodeId + "\'\n" +
                values().stream().map(Node::toString).collect(Collectors.joining("\n")) +
                '}';
        }
    }

    final class Node {
        final private String id;
        final private String nodeName;
        final private Version version;

        Node(String id, String nodeName, Version version) {
            this.id = id;
            this.nodeName = nodeName;
            this.version = version;
        }

        public String getId() {
            return id;
        }

        public String getNodeName() {
            return nodeName;
        }

        public Version getVersion() {
            return version;
        }

        @Override
        public String toString() {
            return "Node{" +
                "id='" + id + '\'' +
                ", nodeName='" + nodeName + '\'' +
                ", version=" + version +
                '}';
        }
    }
}
