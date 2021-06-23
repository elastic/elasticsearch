/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.apache.http.client.fluent.Request;
import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Platforms;
import org.junit.Before;

import java.nio.file.Files;
import java.util.List;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.ServerUtils.makeRequest;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assume.assumeFalse;

public class ConfigurationTests extends PackagingTestCase {

    @Before
    public void filterDistros() {
        assumeFalse("no docker", distribution.isDocker());
    }

    public void test10Install() throws Exception {
        install();
    }

    public void test50StrictDuplicateDetectionDeprecationWarning() throws Exception {
        withCustomConfig(tempConf -> {
            final List<String> jvmOptions = org.elasticsearch.core.List.of("-Des.xcontent.strict_duplicate_detection=false");
            Files.write(tempConf.resolve("jvm.options"), jvmOptions, CREATE, APPEND);
            startElasticsearch();
            stopElasticsearch();
        });
        assertThat(
            FileUtils.slurp(installation.logs.resolve("elasticsearch_deprecation.log")),
            containsString("The Java option es.xcontent.strict_duplicate_detection is set")
        );
    }

    public void test60HostnameSubstitution() throws Exception {
        String hostnameKey = Platforms.WINDOWS ? "COMPUTERNAME" : "HOSTNAME";
        sh.getEnv().put(hostnameKey, "mytesthost");
        withCustomConfig(confPath -> {
            FileUtils.append(confPath.resolve("elasticsearch.yml"), "node.name: ${HOSTNAME}");
            if (distribution.isPackage()) {
                append(installation.envFile, "HOSTNAME=mytesthost");
            }
            assertWhileRunning(() -> {
                final String nameResponse = makeRequest(Request.Get("http://localhost:9200/_cat/nodes?h=name")).trim();
                assertThat(nameResponse, equalTo("mytesthost"));
            });
        });
    }
}
