/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.autoscaling.action;

import org.apache.lucene.util.Constants;
import org.elasticsearch.bootstrap.JavaVersion;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.autoscaling.AutoscalingIntegTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.hamcrest.Matchers;

import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.apache.lucene.util.Constants.JVM_SPEC_VERSION;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class TransportGetAutoscalingCapacityActionIT extends AutoscalingIntegTestCase {

    public void testCurrentCapacity() throws Exception {
        boolean looksLikeDebian8 = Constants.LINUX && Constants.OS_VERSION.startsWith("3.16.0");
        boolean java15Plus = JavaVersion.current().compareTo(JavaVersion.parse("15")) >= 0;
        // see: https://github.com/elastic/elasticsearch/issues/67089#issuecomment-756114654
        assumeTrue("cannot run on debian 8 prior to java 15", java15Plus || looksLikeDebian8 == false);

        assertThat(capacity().results().keySet(), Matchers.empty());
        long memory = OsProbe.getInstance().getTotalPhysicalMemorySize();
        long storage = internalCluster().getInstance(NodeEnvironment.class).nodePaths()[0].fileStore.getTotalSpace();
        assertThat(memory, greaterThan(0L));
        assertThat(storage, greaterThan(0L));
        putAutoscalingPolicy("test");
        assertCurrentCapacity(0, 0, 0);

        int nodes = between(1, 5);
        internalCluster().startDataOnlyNodes(nodes);

        assertBusy(() -> { assertCurrentCapacity(memory, storage, nodes); });
    }

    public void assertCurrentCapacity(long memory, long storage, int nodes) {
        GetAutoscalingCapacityAction.Response capacity = capacity();
        AutoscalingCapacity currentCapacity = capacity.results().get("test").currentCapacity();
        assertThat(currentCapacity.node().memory().getBytes(), Matchers.equalTo(memory));
        assertThat(currentCapacity.total().memory().getBytes(), Matchers.equalTo(memory * nodes));
        assertThat(currentCapacity.node().storage().getBytes(), Matchers.equalTo(storage));
        assertThat(currentCapacity.total().storage().getBytes(), Matchers.equalTo(storage * nodes));
    }

    public GetAutoscalingCapacityAction.Response capacity() {
        GetAutoscalingCapacityAction.Request request = new GetAutoscalingCapacityAction.Request();
        GetAutoscalingCapacityAction.Response response = client().execute(GetAutoscalingCapacityAction.INSTANCE, request).actionGet();
        return response;
    }

    private void putAutoscalingPolicy(String policyName) {
        final PutAutoscalingPolicyAction.Request request = new PutAutoscalingPolicyAction.Request(
            policyName,
            new TreeSet<>(Set.of("data")),
            new TreeMap<>()
        );
        assertAcked(client().execute(PutAutoscalingPolicyAction.INSTANCE, request).actionGet());
    }

}
