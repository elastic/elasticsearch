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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class ResetBuiltinPasswordToolIT extends AbstractPasswordToolTestCase {

    @SuppressWarnings("unchecked")
    public void testResetElasticPasswordTool() throws Exception {
        final String userParameter = randomFrom("--elastic", "--kibana_system");
        final String user = userParameter.substring(2);
        MockTerminal mockTerminal = new MockTerminal();
        ResetBuiltinPasswordTool resetBuiltinPasswordTool = new ResetBuiltinPasswordTool();
        final int status;
        final String password;
        if (randomBoolean()) {
            possiblyDecryptKeystore(mockTerminal);
            status = resetBuiltinPasswordTool.main(new String[] { "-a", "-b", userParameter }, mockTerminal);
            password = readPasswordFromOutput(mockTerminal.getOutput());
        } else {
            password = randomAlphaOfLengthBetween(14, 20);
            possiblyDecryptKeystore(mockTerminal);
            mockTerminal.addSecretInput(password);
            mockTerminal.addSecretInput(password);
            status = resetBuiltinPasswordTool.main(new String[] { "-i", "-b", userParameter }, mockTerminal);
        }
        logger.info("CLI TOOL OUTPUT:\n{}", mockTerminal.getOutput());
        assertEquals(0, status);
        final String basicHeader = "Basic " + Base64.getEncoder().encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
        try {
            Request request = new Request("GET", "/_security/_authenticate");
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Authorization", basicHeader);
            request.setOptions(options);
            Map<String, Object> userInfoMap = entityAsMap(client().performRequest(request));
            assertEquals(user, userInfoMap.get("username"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String readPasswordFromOutput(String output) {
        String[] lines = output.split("\\n");
        for (String line : lines) {
            if (line.startsWith("New value: ")) {
                return line.substring(11);
            }
        }
        return null;
    }

}
