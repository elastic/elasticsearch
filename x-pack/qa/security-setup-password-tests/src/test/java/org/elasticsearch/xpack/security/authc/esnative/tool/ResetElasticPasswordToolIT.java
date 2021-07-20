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

public class ResetElasticPasswordToolIT extends AbstractPasswordToolTestCase {

    @SuppressWarnings("unchecked")
    public void testResetElasticPasswordTool() throws Exception {
        MockTerminal mockTerminal = new MockTerminal();
        ResetElasticPasswordTool resetElasticPasswordTool = new ResetElasticPasswordTool();
        final int status;
        final String password;
        if (randomBoolean()) {
            possiblyDecryptKeystore(mockTerminal);
            status = resetElasticPasswordTool.main(new String[] { "-a", "-b" }, mockTerminal);
            password = readPasswordFromOutput(mockTerminal.getOutput());
        } else {
            password = randomAlphaOfLengthBetween(14, 20);
            possiblyDecryptKeystore(mockTerminal);
            mockTerminal.addSecretInput(password);
            mockTerminal.addSecretInput(password);
            status = resetElasticPasswordTool.main(new String[] { "-i", "-b" }, mockTerminal);
        }
        logger.info("CLI TOOL OUTPUT:\n{}", mockTerminal.getOutput());
        assertEquals(0, status);
        final String basicHeader = "Basic " + Base64.getEncoder().encodeToString(("elastic:" + password).getBytes(StandardCharsets.UTF_8));
        try {
            Request request = new Request("GET", "/_security/_authenticate");
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Authorization", basicHeader);
            request.setOptions(options);
            Map<String, Object> userInfoMap = entityAsMap(client().performRequest(request));
            assertEquals("elastic", userInfoMap.get("username"));
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
