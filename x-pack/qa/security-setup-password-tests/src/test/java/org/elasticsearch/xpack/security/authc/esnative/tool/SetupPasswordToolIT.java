/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative.tool;

import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

public class SetupPasswordToolIT extends ESRestTestCase {

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", token)
                .build();
    }

    @SuppressWarnings("unchecked")
    public void testSetupPasswordToolAutoSetup() throws Exception {
        final String testConfigDir = System.getProperty("tests.config.dir");
        logger.info("--> CONF: {}", testConfigDir);
        final Path configPath = PathUtils.get(testConfigDir);
        setSystemPropsForTool(configPath);

        Response nodesResponse = client().performRequest(new Request("GET", "/_nodes/http"));
        Map<String, Object> nodesMap = entityAsMap(nodesResponse);

        Map<String,Object> nodes = (Map<String,Object>) nodesMap.get("nodes");
        Map<String, Object> firstNode = (Map<String,Object>) nodes.entrySet().iterator().next().getValue();
        Map<String, Object> firstNodeHttp = (Map<String,Object>) firstNode.get("http");
        String nodePublishAddress = (String) firstNodeHttp.get("publish_address");
        final int lastColonIndex = nodePublishAddress.lastIndexOf(':');
        InetAddress actualPublishAddress = InetAddresses.forString(nodePublishAddress.substring(0, lastColonIndex));
        InetAddress expectedPublishAddress = new NetworkService(Collections.emptyList()).resolvePublishHostAddresses(Strings.EMPTY_ARRAY);
        final int port = Integer.valueOf(nodePublishAddress.substring(lastColonIndex + 1));

        List<String> lines = Files.readAllLines(configPath.resolve("elasticsearch.yml"));
        lines = lines.stream().filter(s -> s.startsWith("http.port") == false && s.startsWith("http.publish_port") == false)
                .collect(Collectors.toList());
        lines.add(randomFrom("http.port", "http.publish_port") + ": " + port);
        if (expectedPublishAddress.equals(actualPublishAddress) == false) {
            lines.add("http.publish_address: " + InetAddresses.toAddrString(actualPublishAddress));
        }
        Files.write(configPath.resolve("elasticsearch.yml"), lines, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING);

        MockTerminal mockTerminal = new MockTerminal();
        SetupPasswordTool tool = new SetupPasswordTool();
        final int status;
        if (randomBoolean()) {
            mockTerminal.addTextInput("y"); // answer yes to continue prompt
            status = tool.main(new String[] { "auto" }, mockTerminal);
        } else {
            status = tool.main(new String[] { "auto", "--batch" }, mockTerminal);
        }
        assertEquals(0, status);
        String output = mockTerminal.getOutput();
        logger.info("CLI TOOL OUTPUT:\n{}", output);
        String[] outputLines = output.split("\\n");
        Map<String, String> userPasswordMap = new HashMap<>();
        Arrays.asList(outputLines).forEach(line -> {
            if (line.startsWith("PASSWORD ")) {
                String[] pieces = line.split(" ");
                String user = pieces[1];
                String password = pieces[pieces.length - 1];
                logger.info("user [{}] password [{}]", user, password);
                userPasswordMap.put(user, password);
            }
        });

        assertEquals(7, userPasswordMap.size());
        userPasswordMap.entrySet().forEach(entry -> {
            final String basicHeader = "Basic " +
                    Base64.getEncoder().encodeToString((entry.getKey() + ":" + entry.getValue()).getBytes(StandardCharsets.UTF_8));
            try {
                Request request = new Request("GET", "/_security/_authenticate");
                RequestOptions.Builder options = request.getOptions().toBuilder();
                options.addHeader("Authorization", basicHeader);
                if ("kibana".equals(entry.getKey())) {
                    // the kibana user is deprecated so a warning header is expected
                    options.setWarningsHandler(WarningsHandler.PERMISSIVE);
                }
                request.setOptions(options);
                Map<String, Object> userInfoMap = entityAsMap(client().performRequest(request));
                assertEquals(entry.getKey(), userInfoMap.get("username"));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @SuppressForbidden(reason = "need to set sys props for CLI tool")
    private void setSystemPropsForTool(Path configPath) {
        System.setProperty("es.path.conf", configPath.toString());
        System.setProperty("es.path.home", configPath.getParent().toString());
    }
}
