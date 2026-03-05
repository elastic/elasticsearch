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

package org.elasticsearch.test.cluster.stateless.local;

import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.LocalNodeSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;

import java.util.function.Consumer;

/**
 * Default configuration applied to all stateless clusters.
 */
public class DefaultStatelessLocalConfigProvider implements LocalClusterConfigProvider {
    private final boolean addDefaultNodes;

    public DefaultStatelessLocalConfigProvider(boolean addDefaultNodes) {
        this.addDefaultNodes = addDefaultNodes;
    }

    @Override
    public void apply(LocalClusterSpecBuilder<?> builder) {
        builder.distribution(DistributionType.DEFAULT)
            .keystore("bootstrap.password", "x-pack-test-password")
            .setting("stateless.object_store.type", "fs")
            .setting("stateless.object_store.bucket", "stateless")
            .setting("stateless.object_store.base_path", "base_path")
            .setting("ingest.geoip.downloader.enabled", "false")
            .setting("telemetry.agent.disable_send", "true")
            .feature(FeatureFlag.TIME_SERIES_MODE);
        if (addDefaultNodes) {
            builder.withNode(node("index", "[master,remote_cluster_client,ingest,index]"))
                .withNode(node("search", "[remote_cluster_client,search]"));
        }
    }

    public static Consumer<? super LocalNodeSpecBuilder> node(String name, String nodeRoles) {
        return indexNodeSpec -> indexNodeSpec.name(name)
            .setting("node.roles", nodeRoles)
            .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
            .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB")
            .setting("stateless.translog.flush.interval", "20ms");
    }
}
