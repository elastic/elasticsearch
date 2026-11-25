/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Checks that a strict columnar index (<code>columnar</code> or <code>logsdb_columnar</code>) keeps the sequence number behavior it was
 * created with after an upgrade, whatever that behavior was: the effective {@code index.disable_sequence_numbers} must not change, and
 * the documents must stay readable by id.
 *
 * <p>The case that matters most is an index created by 9.5.2 or later, which disabled sequence numbers for every such index without
 * storing the setting. For it, the old cluster waits for the peer recovery retention lease to advance and force merges the index, so
 * that the merge prunes {@code _seq_no} from the documents below the minimum retained sequence number. The upgraded cluster then has
 * to look the pruned documents up by id. If it opens the index with sequence numbers enabled, a realtime get treats the missing
 * {@code _seq_no} doc value as an error and fails with a 500 (or, with assertions enabled as on a test cluster, an assertion error
 * that stops the node). Note that the index version of those indices, <code>9_111_0_xx</code>, is numerically above the version that
 * gates the default on the main branch, even though the 9.5 branch never had that gate, which is why only a test that starts from a
 * real 9.5.2+ index catches this. Old versions that keep sequence numbers have nothing to prune, so the wait is skipped for them.
 */
public class ColumnarSequenceNumbersFullClusterRestartIT extends ParameterizedFullClusterRestartTestCase {

    /** Documents indexed before the retention lease is advanced, which the merge prunes. */
    private static final int PRUNED_DOCS = 300;
    /** Documents indexed after the retention lease was advanced, which the merge keeps with their {@code _seq_no}. */
    private static final int RETAINED_DOCS = 10;
    private static final int BULK_SIZE = 100;

    /** The effective {@code index.disable_sequence_numbers} the old cluster reported per index, to compare with after the upgrade. */
    private static final Map<String, String> OLD_DISABLE_SEQUENCE_NUMBERS = new ConcurrentHashMap<>();

    @ClassRule
    public static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(OLD_CLUSTER_VERSION, isOldClusterDetachedVersion())
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.ml.enabled", "false")
        .build();

    public ColumnarSequenceNumbersFullClusterRestartIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    public void testColumnar() throws Exception {
        testPrunedSequenceNumbers("fcr-columnar-seq-no", "columnar");
    }

    public void testLogsdbColumnar() throws Exception {
        testPrunedSequenceNumbers("fcr-logsdb-columnar-seq-no", "logsdb_columnar");
    }

    private void testPrunedSequenceNumbers(String index, String indexMode) throws Exception {
        if (isRunningAgainstOldCluster()) {
            assumeTrue(
                "the old cluster does not support columnar index modes",
                clusterHasCapability("PUT", "/{index}", List.of(), List.of("columnar_index_modes")).orElse(false)
            );
            createIndex(index, indexMode);
            for (int first = 0; first < PRUNED_DOCS; first += BULK_SIZE) {
                bulkIndex(index, first, BULK_SIZE);
            }
            String disabled = disableSequenceNumbers(index);
            OLD_DISABLE_SEQUENCE_NUMBERS.put(index, disabled);
            if (Boolean.parseBoolean(disabled)) {
                // The lease is synced on a fixed interval, 30 seconds by default, which can't be changed on a real cluster. Without
                // sequence numbers the merge below only prunes once the lease has advanced; with them there is nothing to prune.
                assertBusy(() -> assertThat(retainingSeqNo(index), greaterThanOrEqualTo((long) PRUNED_DOCS)), 2, TimeUnit.MINUTES);
            }
            // flush so the safe commit covers the first documents, then index the ones that must keep their _seq_no
            assertOK(client().performRequest(new Request("POST", "/" + index + "/_flush")));
            bulkIndex(index, PRUNED_DOCS, RETAINED_DOCS);
            // the merge removes the _seq_no of every document below the minimum retained sequence number
            Request forceMerge = new Request("POST", "/" + index + "/_forcemerge");
            forceMerge.addParameter("max_num_segments", "1");
            assertOK(client().performRequest(forceMerge));
            assertOK(client().performRequest(new Request("POST", "/" + index + "/_flush")));
            assertRealtimeGets(index);
        } else {
            assumeTrue("the old cluster did not create the index", indexExists(index));
            String oldDisabled = OLD_DISABLE_SEQUENCE_NUMBERS.get(index);
            assertThat("the old cluster run did not record the setting of [" + index + "]", oldDisabled, notNullValue());
            ensureGreen(index);
            // The index has to keep the behavior it was created with. This is checked before the gets on purpose: with assertions
            // enabled, as on a test cluster, a realtime get of a pruned document trips an assert in PerThreadIDVersionAndSeqNoLookup,
            // which is fatal and takes the node down, whereas a production build answers with a 500.
            assertThat(disableSequenceNumbers(index), equalTo(oldDisabled));
            assertRealtimeGets(index);
            Request update = new Request("POST", "/" + index + "/_update/doc-0");
            update.addParameter("refresh", "true");
            update.setJsonEntity("{\"doc\": {\"message\": \"updated\"}}");
            if (Boolean.parseBoolean(oldDisabled)) {
                // updates need sequence numbers, so they are rejected, as they were on the old cluster
                ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(update));
                assertThat(e.getMessage(), e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
                assertThat(e.getMessage(), containsString("Updates are not supported on indices with sequence numbers disabled"));
            } else {
                assertOK(client().performRequest(update));
                assertThat(ObjectPath.evaluate(get(index, "doc-0"), "_source.message"), equalTo("updated"));
            }
        }
    }

    /** Gets a pruned and a retained document by id, by every realtime get API. */
    private static void assertRealtimeGets(String index) throws IOException {
        for (String id : List.of("doc-0", "doc-" + (PRUNED_DOCS - 1), "doc-" + PRUNED_DOCS, "doc-" + (PRUNED_DOCS + RETAINED_DOCS - 1))) {
            assertThat("get [" + id + "]", ObjectPath.evaluate(get(index, id), "found"), equalTo(true));
            assertOK(client().performRequest(new Request("GET", "/" + index + "/_source/" + id)));
            assertOK(client().performRequest(new Request("HEAD", "/" + index + "/_doc/" + id)));
        }
        Request mget = new Request("POST", "/" + index + "/_mget");
        mget.setJsonEntity("{\"ids\": [\"doc-0\", \"doc-" + PRUNED_DOCS + "\"]}");
        Map<String, Object> mgetResponse = entityAsMap(client().performRequest(mget));
        assertThat(mgetResponse.toString(), ObjectPath.evaluate(mgetResponse, "docs.0.found"), equalTo(true));
        assertThat(mgetResponse.toString(), ObjectPath.evaluate(mgetResponse, "docs.1.found"), equalTo(true));
    }

    private static Map<String, Object> get(String index, String id) throws IOException {
        return entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_doc/" + id)));
    }

    private void createIndex(String index, String indexMode) throws IOException {
        Request request = new Request("PUT", "/" + index);
        request.setJsonEntity(Strings.format("""
            {
              "settings": {
                "index.mode": "%s",
                "index.number_of_shards": 1,
                "index.number_of_replicas": 0,
                "index.soft_deletes.retention.operations": 0
              },
              "mappings": {
                "properties": {
                  "@timestamp": { "type": "date" },
                  "host.name": { "type": "keyword" },
                  "message": { "type": "keyword" }
                }
              }
            }""", indexMode));
        assertOK(client().performRequest(request));
        ensureGreen(index);
    }

    private static void bulkIndex(String index, int firstDoc, int count) throws IOException {
        StringBuilder bulk = new StringBuilder();
        for (int i = firstDoc; i < firstDoc + count; i++) {
            bulk.append(Strings.format("""
                {"index":{"_index":"%s","_id":"doc-%d"}}
                {"@timestamp":"2024-06-01T10:%02d:%02dZ","host.name":"host-%d","message":"doc %d"}
                """, index, i, (i / 60) % 60, i % 60, i % 3, i));
        }
        Request request = new Request("POST", "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(bulk.toString());
        Response response = client().performRequest(request);
        assertOK(response);
        assertThat(ObjectPath.evaluate(entityAsMap(response), "errors"), equalTo(false));
    }

    /** The sequence number the lowest retention lease of the only shard retains, which only moves when the leases are synced. */
    private static long retainingSeqNo(String index) throws IOException {
        Request request = new Request("GET", "/" + index + "/_stats");
        request.addParameter("level", "shards");
        request.addParameter("filter_path", "indices.*.shards.*.retention_leases.leases.retaining_seq_no");
        Map<String, Object> stats = entityAsMap(client().performRequest(request));
        long min = Long.MAX_VALUE;
        Map<String, Object> indices = ObjectPath.evaluate(stats, "indices");
        for (Object indexStats : indices.values()) {
            Map<String, Object> shards = ObjectPath.evaluate(indexStats, "shards");
            for (Object copies : shards.values()) {
                for (Object copy : (List<?>) copies) {
                    for (Object lease : (List<?>) ObjectPath.evaluate(copy, "retention_leases.leases")) {
                        min = Math.min(min, ((Number) ObjectPath.evaluate(lease, "retaining_seq_no")).longValue());
                    }
                }
            }
        }
        return min;
    }

    private static String disableSequenceNumbers(String index) throws IOException {
        Request request = new Request("GET", "/" + index + "/_settings");
        request.addParameter("include_defaults", "true");
        request.addParameter("flat_settings", "true");
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        Map<String, Object> defaults = ObjectPath.evaluate(response, index + ".defaults");
        Map<String, Object> settings = ObjectPath.evaluate(response, index + ".settings");
        // the effective value is the stored one when there is one, and otherwise the default, which is all a 9.5.2+ index has
        Object explicit = settings.get("index.disable_sequence_numbers");
        return (String) (explicit != null ? explicit : defaults.get("index.disable_sequence_numbers"));
    }
}
