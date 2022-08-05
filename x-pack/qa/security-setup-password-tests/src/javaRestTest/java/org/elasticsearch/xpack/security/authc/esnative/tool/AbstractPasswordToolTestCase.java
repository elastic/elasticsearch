/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.esnative.tool;

import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractPasswordToolTestCase extends ESRestTestCase {

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    @SuppressWarnings("unchecked")
    void writeConfigurationToDisk() throws Exception {
        final String testConfigDir = System.getProperty("tests.config.dir");
        logger.info("--> CONF: {}", testConfigDir);
        final Path configPath = PathUtils.get(testConfigDir);
        setSystemPropsForTool(configPath);

        Response nodesResponse = client().performRequest(new Request("GET", "/_nodes/http"));
        Map<String, Object> nodesMap = entityAsMap(nodesResponse);

        Map<String, Object> nodes = (Map<String, Object>) nodesMap.get("nodes");
        Map<String, Object> firstNode = (Map<String, Object>) nodes.entrySet().iterator().next().getValue();
        Map<String, Object> firstNodeHttp = (Map<String, Object>) firstNode.get("http");
        String nodePublishAddress = (String) firstNodeHttp.get("publish_address");
        final int lastColonIndex = nodePublishAddress.lastIndexOf(':');
        InetAddress actualPublishAddress = InetAddresses.forString(nodePublishAddress.substring(0, lastColonIndex));
        InetAddress expectedPublishAddress = new NetworkService(Collections.emptyList()).resolvePublishHostAddresses(Strings.EMPTY_ARRAY);
        final int port = Integer.valueOf(nodePublishAddress.substring(lastColonIndex + 1));

        List<String> lines = Files.readAllLines(configPath.resolve("elasticsearch.yml"));
        lines = lines.stream()
            .filter(s -> s.startsWith("http.port") == false && s.startsWith("http.publish_port") == false)
            .collect(Collectors.toList());
        lines.add(randomFrom("http.port", "http.publish_port") + ": " + port);
        if (expectedPublishAddress.equals(actualPublishAddress) == false) {
            lines.add("http.publish_address: " + InetAddresses.toAddrString(actualPublishAddress));
        }
        Files.write(configPath.resolve("elasticsearch.yml"), lines, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);
    }

    protected void possiblyDecryptKeystore(MockTerminal mockTerminal) {
        if (inFipsJvm()) {
            // In our FIPS 140-2 tests, we set the keystore password to `keystore-password`
            mockTerminal.addSecretInput("keystore-password");
        }
    }

    @SuppressForbidden(reason = "need to set sys props for CLI tool")
    void setSystemPropsForTool(Path configPath) {
        System.setProperty("es.path.conf", configPath.toString());
        System.setProperty("es.path.home", configPath.getParent().toString());
    }

    protected ProcessInfo getToolProcessInfo() {
        var sysprops = Map.of("es.path.conf", System.getProperty("es.path.conf"), "es.path.home", System.getProperty("es.path.home"));
        return new ProcessInfo(sysprops, Map.of(), createTempDir());
    }
}
