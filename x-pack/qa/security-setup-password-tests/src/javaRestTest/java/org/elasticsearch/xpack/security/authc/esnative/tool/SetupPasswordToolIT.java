/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.esnative.tool;

import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.WarningsHandler;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class SetupPasswordToolIT extends AbstractPasswordToolTestCase {

    public void testSetupPasswordToolAutoSetup() throws Exception {

        MockTerminal mockTerminal = MockTerminal.create();
        SetupPasswordTool tool = new SetupPasswordTool();
        final int status;
        if (randomBoolean()) {
            possiblyDecryptKeystore(mockTerminal);
            mockTerminal.addTextInput("y"); // answer yes to continue prompt
            status = tool.main(new String[] { "auto" }, mockTerminal, getToolProcessInfo());
        } else {
            possiblyDecryptKeystore(mockTerminal);
            status = tool.main(new String[] { "auto", "--batch" }, mockTerminal, getToolProcessInfo());
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
            final String basicHeader = "Basic "
                + Base64.getEncoder().encodeToString((entry.getKey() + ":" + entry.getValue()).getBytes(StandardCharsets.UTF_8));
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

}
