/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.shutdown.masterfailover;

import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.stateless.StatelessElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;

import java.io.IOException;

import static org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService.MAX_MISSED_HEARTBEATS;

public class MasterFailoverIT extends ESRestTestCase {

    @ClassRule
    public static StatelessElasticsearchCluster cluster = StatelessElasticsearchCluster.local()
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .user("admin-user", "x-pack-test-password")
        .setting(MAX_MISSED_HEARTBEATS.getKey(), "100") // blocks non-graceful failovers for long enough that the test will fail
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin-user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testFailover() throws Exception {
        var client = client();

        final var indexName = randomIdentifier();

        final var indexRequest = new Request("POST", indexName + "/" + "_doc/");
        indexRequest.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("f", "v").endObject()));
        assertOK(client.performRequest(indexRequest));

        final long primaryTerm = getFirstShardPrimaryTerm(client, indexName);

        final var getMasterNodeResponse = assertOKAndCreateObjectPath(client.performRequest(new Request("GET", "_nodes/_master/_none")));
        final var masterNodeId = (String) getMasterNodeResponse.evaluate("nodes._arbitrary_key_");
        final var masterNodeName = (String) getMasterNodeResponse.evaluate("nodes." + masterNodeId + ".name");
        final var masterNodeIndex = nodeIndexFromName(masterNodeName);

        final var upgradeResult = new PlainActionFuture<Void>();
        final var upgradeThread = new Thread(
            ActionRunnable.run(upgradeResult, () -> cluster.upgradeNodeToVersion(masterNodeIndex, Version.CURRENT)),
            "upgrade-thread"
        );
        upgradeThread.start();

        try {
            while (upgradeResult.isDone() == false) {
                // Verifies that we barely notice the master failover, although using a somewhat generous 10s timeout just in case the
                // CI worker is very slow. Still, 10s is shorter than the default 30s timeout on things like master node requests.
                final var healthResponse = assertOKAndCreateObjectPath(
                    client.performRequest(new Request("GET", "_cluster/health?master_timeout=10s"))
                );
                assertFalse(healthResponse.toString(), healthResponse.evaluate("timed_out"));
            }
        } finally {
            upgradeThread.join();
        }

        assertNull(upgradeResult.get());

        // The primary term would change if (a) the primary shard did not relocate gracefully, or (b) the master failover reopened the
        // routing table from scratch, so this assertion verifies that neither of the above happened:
        assertEquals(primaryTerm, getFirstShardPrimaryTerm(client, indexName));
    }

    private static int getFirstShardPrimaryTerm(RestClient client, String indexName) throws IOException {
        return assertOKAndCreateObjectPath(client.performRequest(new Request("GET", "_cluster/state"))).evaluate(
            "metadata.indices." + indexName + ".primary_terms.0"
        );
    }

    private static int nodeIndexFromName(String targetName) {
        for (int i = 0;; i++) {
            if (targetName.equals(getNodeNameSafe(i))) {
                return i;
            }
        }
    }

    private static String getNodeNameSafe(int i) {
        try {
            return cluster.getName(i);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
