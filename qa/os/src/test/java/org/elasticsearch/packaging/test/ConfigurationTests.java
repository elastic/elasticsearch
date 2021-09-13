/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.apache.http.client.fluent.Request;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.junit.Before;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
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
            Path yml = confPath.resolve("elasticsearch.yml");
            List<String> lines;
            try (Stream<String> allLines = Files.readAllLines(yml).stream()) {
                lines = allLines.map(l -> {
                    if (l.contains(installation.config.toString())) {
                        return l.replace(installation.config.toString(), confPath.toString());
                    }
                    return l;
                }).collect(Collectors.toList());
            }
            lines.add("node.name: ${HOSTNAME}");
            Files.write(yml, lines, TRUNCATE_EXISTING);
            logger.debug("TEMP_IOANNIS " + Files.readString(confPath.resolve("elasticsearch.yml")));
            logger.debug("TEMP_IOANNIS" + Files.readString(ServerUtils.getCaCert(installation)));
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
                    ServerUtils.getCaCert(installation)
                ).strip();
                assertThat(nameResponse, equalTo("mytesthost"));
            });
        });
    }
}
