/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.docker.test;

import org.elasticsearch.test.fixtures.testcontainers.DockerEnvironmentAwareTestContainer;
import org.testcontainers.containers.BindMode;
import org.testcontainers.images.builder.ImageFromDockerfile;

public class ElasticsearchDockerDistributionTestContainer extends DockerEnvironmentAwareTestContainer {

    private static final String DOCKER_BASE_IMAGE = "elasticsearch:test";

    public ElasticsearchDockerDistributionTestContainer() {
        super(
            new ImageFromDockerfile("elasticsearch").withDockerfileFromBuilder(
                    builder -> builder.from(DOCKER_BASE_IMAGE)
                        .env("node.name","elasticsearch-default-1")
                        .env("cluster.initial_master_nodes","elasticsearch-default-1,elasticsearch-default-2")
                        .env("discovery.seed_hosts","elasticsearch-default-2:9300")
                        .env("cluster.name","elasticsearch-default")
                        .env("bootstrap.memory_lock","true")
                        .env("ES_JAVA_OPTS","-Xms512m -Xmx512m")
                        .env("path.repo","/tmp/es-repo")
                        .env("node.attr.testattr","test")
                        .env("cluster.routing.allocation.disk.watermark.low","1b")
                        .env("cluster.routing.allocation.disk.watermark.high","1b")
                        .env("cluster.routing.allocation.disk.watermark.flood_stage","1b")
                        .env("node.store.allow_mmap","false")
                        .env("ingest.geoip.downloader.enabled","false")
                        .env("xpack.security.enabled","true")
                        .env("xpack.security.transport.ssl.enabled","true")
                        .env("xpack.security.http.ssl.enabled","true")
                        .env("xpack.security.authc.token.enabled","true")
                        .env("xpack.security.audit.enabled","true")
                        .env("xpack.security.authc.realms.file.file1.order","0")
                        .env("xpack.security.authc.realms.native.native1.order","1")
                        .env("xpack.security.transport.ssl.key","/usr/share/elasticsearch/config/testnode.pem")
                        .env("xpack.security.transport.ssl.certificate","/usr/share/elasticsearch/config/testnode.crt")
                        .env("xpack.security.http.ssl.key","/usr/share/elasticsearch/config/testnode.pem")
                        .env("xpack.security.http.ssl.certificate","/usr/share/elasticsearch/config/testnode.crt")
                        .env("xpack.http.ssl.verification_mode","certificate")
                        .env("xpack.security.transport.ssl.verification_mode","certificate")
                        .env("xpack.license.self_generated.type","trial")
                        .env("action.destructive_requires_name","false")
                        .env("cluster.deprecation_indexing.enabled","false")
                        .entryPoint("/docker-test-entrypoint.sh")
                        .build()


                //    volumes:
                //       - ./testfixtures_shared/repo:/tmp/es-repo
                //       - ./build/certs/testnode.pem:/usr/share/elasticsearch/config/testnode.pem
                //       - ./build/certs/testnode.crt:/usr/share/elasticsearch/config/testnode.crt
                //       - ./testfixtures_shared/logs/default-1:/usr/share/elasticsearch/logs
                //       - ./docker-test-entrypoint.sh:/docker-test-entrypoint.sh
                )
                .withFileFromClasspath("/usr/share/elasticsearch/config/testnode.pem", "/testnode.pem")
                .withFileFromClasspath("/usr/share/elasticsearch/config/testnode.crt", "/testnode.crt")
                .withFileFromClasspath("/docker-test-entrypoint.sh", "/docker-test-entrypoint.sh")

        );
//        addFileSystemBind("testfixtures_shared/repo", "/tmp/es-repo", BindMode.READ_WRITE);
//        addFileSystemBind("/testfixtures_shared/logs/default-1", "/usr/share/elasticsearch/logs", BindMode.READ_WRITE);
        addExposedPort(9200);
    }
}
