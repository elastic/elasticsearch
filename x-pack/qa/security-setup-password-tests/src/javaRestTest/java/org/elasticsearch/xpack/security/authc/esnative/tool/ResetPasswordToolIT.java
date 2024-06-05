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
import org.elasticsearch.client.Response;
import org.elasticsearch.core.Strings;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ResetPasswordToolIT extends AbstractPasswordToolTestCase {

    @SuppressWarnings("unchecked")
    public void testResetBuiltinUser() throws Exception {
        final String user = randomFrom(
            "elastic",
            "kibana_system",
            "logstash_system",
            "apm_system",
            "beats_system",
            "remote_monitoring_user"
        );
        MockTerminal mockTerminal = MockTerminal.create();
        ResetPasswordTool resetPasswordTool = new ResetPasswordTool();
        final int status;
        final String password;
        if (randomBoolean()) {
            possiblyDecryptKeystore(mockTerminal);
            status = resetPasswordTool.main(new String[] { "-a", "-b", "-u", user }, mockTerminal, getToolProcessInfo());
            password = readPasswordFromOutput(mockTerminal.getOutput());
        } else {
            password = randomAlphaOfLengthBetween(14, 20);
            possiblyDecryptKeystore(mockTerminal);
            mockTerminal.addSecretInput(password);
            mockTerminal.addSecretInput(password);
            status = resetPasswordTool.main(new String[] { "-i", "-b", "-u", user }, mockTerminal, getToolProcessInfo());
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

    @SuppressWarnings("unchecked")
    public void testResetNativeUser() throws Exception {
        // Create a native user
        final String nativeUser = randomAlphaOfLength(8);
        final String adminBasicHeader = "Basic "
            + Base64.getEncoder().encodeToString(("test_admin:x-pack-test-password").getBytes(StandardCharsets.UTF_8));
        try {
            Request putUserRequest = new Request("PUT", "/_security/user/" + nativeUser);
            putUserRequest.setJsonEntity(Strings.format("""
                {
                   "password" : "l0ng-r4nd0m-p@ssw0rd",
                   "roles" : [ "admin", "other_role1" ],
                   "full_name" : "%s",
                   "email" : "%s@example.com",
                   "enabled": true
                }""", randomAlphaOfLength(5), nativeUser));
            RequestOptions.Builder options = putUserRequest.getOptions().toBuilder();
            options.addHeader("Authorization", adminBasicHeader);
            putUserRequest.setOptions(options);
            final Response putUserResponse = client().performRequest(putUserRequest);
            assertThat(putUserResponse.getStatusLine().getStatusCode(), equalTo(200));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // Verify that we can authenticate as that native user
        final String basicHeader = "Basic "
            + Base64.getEncoder().encodeToString((nativeUser + ":l0ng-r4nd0m-p@ssw0rd").getBytes(StandardCharsets.UTF_8));
        try {
            Request request = new Request("GET", "/_security/_authenticate");
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Authorization", basicHeader);
            request.setOptions(options);
            Map<String, Object> userInfoMap = entityAsMap(client().performRequest(request));
            assertEquals(nativeUser, userInfoMap.get("username"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        // Now change the password
        MockTerminal mockTerminal = MockTerminal.create();
        ResetPasswordTool resetPasswordTool = new ResetPasswordTool();
        final int status;
        final String password;
        if (randomBoolean()) {
            possiblyDecryptKeystore(mockTerminal);
            status = resetPasswordTool.main(new String[] { "-a", "-b", "-u", nativeUser }, mockTerminal, getToolProcessInfo());
            password = readPasswordFromOutput(mockTerminal.getOutput());
        } else {
            password = randomAlphaOfLengthBetween(14, 20);
            possiblyDecryptKeystore(mockTerminal);
            mockTerminal.addSecretInput(password);
            mockTerminal.addSecretInput(password);
            status = resetPasswordTool.main(new String[] { "-i", "-b", "-u", nativeUser }, mockTerminal, getToolProcessInfo());
        }
        logger.info("CLI TOOL OUTPUT:\n{}", mockTerminal.getOutput());
        assertEquals(0, status);

        // and authenticate with the new password
        final String newBasicHeader = "Basic "
            + Base64.getEncoder().encodeToString((nativeUser + ":" + password).getBytes(StandardCharsets.UTF_8));
        try {
            Request request = new Request("GET", "/_security/_authenticate");
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Authorization", newBasicHeader);
            request.setOptions(options);
            Map<String, Object> userInfoMap = entityAsMap(client().performRequest(request));
            assertEquals(nativeUser, userInfoMap.get("username"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String readPasswordFromOutput(String output) {
        return output.lines().filter(line -> line.startsWith("New value: ")).map(line -> line.substring(11)).findFirst().orElse(null);
    }

}
