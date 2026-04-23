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

package org.elasticsearch.xpack.stateless.shutdown;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.cluster.stateless.StatelessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class SigtermShutdownIT extends ESRestTestCase {

    private static final TimeValue SIGTERM_TIMEOUT = new TimeValue(15, TimeUnit.SECONDS);

    @ClassRule
    public static StatelessElasticsearchCluster cluster = StatelessElasticsearchCluster.local()
        .module("stateless")
        .module("stateless-sigterm")
        .setting("xpack.ml.enabled", "false")
        .user("admin-user", "x-pack-test-password")
        .setting("xpack.watcher.enabled", "false")
        .setting("stateless.sigterm.timeout", SIGTERM_TIMEOUT.getStringRep())
        .setting("stateless.sigterm.poll_interval", "1s")
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

    public void testShutdown() throws Exception {
        int searchIndex = 1;
        var name = cluster.getName(searchIndex);
        var nodeId = assertOKAndCreateObjectPath(client().performRequest(new Request("GET", "_nodes/" + name))).evaluate(
            "nodes._arbitrary_key_"
        );
        assertThat(nodeId, instanceOf(String.class));
        var shutdownStatus = new Request("GET", "_nodes/" + nodeId + "/shutdown");
        assertThat(assertOKAndCreateObjectPath(client().performRequest(shutdownStatus)).evaluate("nodes"), hasSize(0));
        cluster.stopNode(searchIndex, false);
        // Created
        assertBusy(
            () -> assertThat(assertOKAndCreateObjectPath(client().performRequest(shutdownStatus)).evaluate("nodes"), hasSize(1)),
            SIGTERM_TIMEOUT.getSeconds(),
            TimeUnit.SECONDS
        );
        // Cleaned up
        assertBusy(
            () -> assertThat(assertOKAndCreateObjectPath(client().performRequest(shutdownStatus)).evaluate("nodes"), hasSize(0)),
            SIGTERM_TIMEOUT.getSeconds(),
            TimeUnit.SECONDS
        );
    }
}
