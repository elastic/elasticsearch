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
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.junit.Before;

import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.ServerUtils.makeRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

public class ConfigurationTests extends PackagingTestCase {

    private static String superuser = "test_superuser";
    private static String superuserPassword = "test_superuser";

    @Before
    public void filterDistros() {
        assumeFalse("no docker", distribution.isDocker());
    }

    public void test10Install() throws Exception {
        install();
        Shell.Result result = sh.run(
            installation.executables().usersTool + " useradd " + superuser + " -p " + superuserPassword + " -r " + "superuser"
        );
        assumeTrue(result.isSuccess());
    }

    public void test60HostnameSubstitution() throws Exception {
        String hostnameKey = Platforms.WINDOWS ? "COMPUTERNAME" : "HOSTNAME";
        sh.getEnv().put(hostnameKey, "mytesthost");
        withCustomConfig(confPath -> {
            FileUtils.append(confPath.resolve("elasticsearch.yml"), "node.name: ${HOSTNAME}");
            if (distribution.isPackage()) {
                append(installation.envFile, "HOSTNAME=mytesthost");
            }
            // Packaged installations don't get autoconfigured yet
            // TODO: Remove this in https://github.com/elastic/elasticsearch/pull/75144
            String protocol = distribution.isPackage() ? "http" : "https";
            assertWhileRunning(() -> {
                final String nameResponse = makeRequest(
                    Request.Get(protocol + "://localhost:9200/_cat/nodes?h=name"),
                    superuser,
                    superuserPassword,
                    ServerUtils.getCaCert(confPath)
                ).strip();
                assertThat(nameResponse, equalTo("mytesthost"));
            });
        });
    }
}
