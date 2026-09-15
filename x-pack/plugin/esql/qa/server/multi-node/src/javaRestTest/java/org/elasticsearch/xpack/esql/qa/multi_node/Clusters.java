/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.datasources.Federation;

import java.nio.file.Path;

public class Clusters {
    public static ElasticsearchCluster testCluster(LocalClusterConfigProvider configProvider) {
        return testCluster(CsvTestUtils.createCsvDataDirectory(), configProvider, false);
    }

    public static ElasticsearchCluster testCluster(Path csvDataPath, LocalClusterConfigProvider configProvider, boolean shared) {
        LocalClusterSpecBuilder<ElasticsearchCluster> cluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .nodes(2)
            .setting("xpack.security.enabled", "false")
            .setting("xpack.license.self_generated.type", "trial")
            .setting("path.repo", csvDataPath::toString)
            .configFile("user-agent/custom-regexes.yml", Resource.fromClasspath("custom-regexes.yml"))
            .configFile("ingest-geoip/GeoLite2-City.mmdb", Resource.fromClasspath("GeoLite2-City.mmdb"))
            .configFile("ingest-geoip/GeoLite2-Country.mmdb", Resource.fromClasspath("GeoLite2-Country.mmdb"))
            .configFile("ingest-geoip/GeoLite2-ASN.mmdb", Resource.fromClasspath("GeoLite2-ASN.mmdb"))
            .setting("ingest.geoip.downloader.enabled", "false");
        // Federation is only on by default in snapshot builds; the EXTERNAL and dataset specs run here need it on.
        // Only where the platform can run it at all: where it cannot, the node registers none of the feature's
        // settings, so writing one into elasticsearch.yml would fail node startup with the framework's "unknown
        // setting" error and take down every suite on this cluster, not just the external ones.
        if (Federation.SUPPORTED) {
            cluster.setting("esql.external.local_allowed_paths", csvDataPath::toString)
                .setting(Federation.FEDERATION_ENABLED.getKey(), "true");
            // Where registration defaults off (Windows) these suites must ask for it. Set only there: elsewhere it
            // would merely restate the default.
            if (Federation.DEFAULT_REGISTERED == false) {
                cluster.systemProperty(Federation.REGISTER_PROPERTY, "true");
            }
        }
        cluster.apply(() -> configProvider);
        if (shared) {
            cluster.shared(true);
        }
        return cluster.build();
    }
}
