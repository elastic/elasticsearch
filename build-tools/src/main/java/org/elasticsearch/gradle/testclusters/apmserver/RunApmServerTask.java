/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.testclusters.apmserver;

import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.elasticsearch.gradle.testclusters.ElasticsearchNode;
import org.elasticsearch.gradle.testclusters.TestClustersAware;
import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;

public class RunApmServerTask extends DefaultTask implements TestClustersAware {

    private Collection<ElasticsearchCluster> clusters = new HashSet<>();

    @Override
    public Collection<ElasticsearchCluster> getClusters() {
        return clusters;
    }
    private MockApmServer mockServer;

    @Override
    public void beforeStart() {
        mockServer = new MockApmServer();


        for (ElasticsearchCluster cluster : getClusters()) {
            for (ElasticsearchNode node : cluster.getNodes()) {
                node.setting("telemetry.metrics.enabled", "true");
                node.setting("tracing.apm.enabled", "true");
                node.setting("tracing.apm.agent.server_url", "http://127.0.0.1:9999");
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            System.out.println("shutdown hook");
            mockServer.stop();
        } ));
    }
    @TaskAction
    public void runAndWait()  {
        try {
            mockServer.start();
            //wait here
            synchronized (this){
                this.wait();
            }
        } catch (IOException e) {
            System.out.println("io");
        } catch (InterruptedException e) {
            System.out.println("interrupted");
        } finally {
            System.out.println("stopping");

            mockServer.stop();
        }
    }


//            mockServer.stop();

}
