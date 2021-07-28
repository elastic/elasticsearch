/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.security.tool.CommandLineHttpClient;
import org.elasticsearch.xpack.security.tool.HttpResponse;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.security.authc.esnative.tool.SetupPasswordTool.getErrorCause;

public class PasswordAndEnrollmentInitialNode extends EnvironmentAwareCommand {
    private static final Setting<SecureString> SEED_SETTING = SecureSetting.secureString("keystore.seed", null);
    private static final Setting<SecureString> BOOTSTRAP_ELASTIC_PASSWORD = SecureSetting.secureString("bootstrap.password",
        null);
    private static final Setting<SecureString> CREDENTIALS_PASSWORD = SecureSetting.secureString("bootstrap.password",
        SEED_SETTING);
    private static final String elasticUser = ElasticUser.NAME;

    private SecureString password;
    private String token;
    private String fingerprint;

    // Protected for testing
    protected SecureString getPassword() {
        return password;
    }

    protected String getToken() {
        return token;
    }

    protected String getFingerprint() {
        return fingerprint;
    }

    protected CommandLineHttpClient getClient(Environment env) {
        return new CommandLineHttpClient(env);
    }

    protected CreateEnrollmentToken getCreateEnrollmentToken(Environment env) throws Exception{
        return new CreateEnrollmentToken(env);
    }


    PasswordAndEnrollmentInitialNode (){
        super("Set elastic password and generate enrollment token for initial node");
        parser.allowsUnrecognizedOptions();
    }

    public static void main(String[] args) throws Exception {
        exit(new PasswordAndEnrollmentInitialNode().main(args, Terminal.DEFAULT));
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        if (options.nonOptionArguments().contains("--explicitly-acknowledge-execution") == false) {
            throw new UserException(ExitCodes.NOOP, "This command is not intended for end users");
        }
        if (env.settings().hasValue(XPackSettings.ENROLLMENT_ENABLED.getKey()) && false ==
            XPackSettings.ENROLLMENT_ENABLED.get(env.settings())) {
            throw new UserException(ExitCodes.NOOP, "Enrollment is explicitly disabled.");
        }
        CommandLineHttpClient client = getClient(env);
        CreateEnrollmentToken cet = getCreateEnrollmentToken(env);

        // Try 5 times and rethrow the last exception from checkClusterHealth
        for (int retry = 5; ; retry--) {
            try {
                checkClusterHealth(env, client);
                break;
            } catch (Exception e) {
                if (retry == 1) {
                    throw e;
                }
                Thread.sleep(1000);
            }
        }
        if (Strings.isNullOrEmpty(BOOTSTRAP_ELASTIC_PASSWORD.get(env.settings()).toString())) {
            changeElasticUserPassword(env, terminal, client);
        } else {
            password = BOOTSTRAP_ELASTIC_PASSWORD.get(env.settings());
        }
        token = cet.createKibanaEnrollmentToken(elasticUser, password);
        Map<String, String> infoNode = getDecoded(token);
        fingerprint = infoNode.get("fgr");

        terminal.println("'elastic' user password: " + password);
        terminal.println("enrollment token: " + token);
        terminal.println("CA fingerprint: " + fingerprint);
    }

    protected void checkClusterHealth(Environment env, CommandLineHttpClient client) throws Exception {
        final URL clusterHealthUrl = checkClusterHealthUrl(client);
        final HttpResponse response;
        try {
            response = client.execute("GET", clusterHealthUrl, elasticUser, CREDENTIALS_PASSWORD.get(env.settings()),
                () -> null, this::responseBuilder);
        } catch (Exception e) {
            throw new UserException(ExitCodes.UNAVAILABLE, "Failed to determine the health of the cluster. ", e);
        }
        final int responseStatus = response.getHttpStatus();
        if (responseStatus != HttpURLConnection.HTTP_OK) {
            throw new UserException(
                ExitCodes.DATA_ERROR,
                "Failed to determine the health of the cluster. Unexpected http status [" + responseStatus + "]"
            );
        } else {
            final String clusterStatus = Objects.toString(response.getResponseBody().get("status"), "");
            if (clusterStatus.isEmpty()) {
                throw new UserException(
                    ExitCodes.DATA_ERROR,
                    "Failed to determine the health of the cluster. Cluster health API did not return a status value."
                );
            } else if ("red".equalsIgnoreCase(clusterStatus)) {
                throw new UserException(ExitCodes.UNAVAILABLE,
                    "Failed to determine the health of the cluster. Cluster health is currently RED.");
            }
        }
    }

    protected void changeElasticUserPassword(Environment env, Terminal terminal, CommandLineHttpClient client) throws Exception {
        final URL passwordChangeUrl = changeElasticUserPasswordUrl(client);
        final HttpResponse response;
        password = new SecureString(generatePassword(20));
        try {
            response = client.execute("POST", passwordChangeUrl, elasticUser, CREDENTIALS_PASSWORD.get(env.settings()),
                () -> {
                    XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
                    xContentBuilder.startObject().field("password", password).endObject();
                    return Strings.toString(xContentBuilder);
                }, this::responseBuilder);
            if (response.getHttpStatus() != HttpURLConnection.HTTP_OK) {
                terminal.errorPrintln("");
                terminal.errorPrintln(
                    "Unexpected response code [" + response.getHttpStatus() + "] from calling PUT " + passwordChangeUrl.toString());
                String cause = getErrorCause(response);
                if (cause != null) {
                    terminal.errorPrintln("Cause: " + cause);
                    terminal.errorPrintln("");
                }
                terminal.errorPrintln("");
                throw new UserException(ExitCodes.TEMP_FAILURE, "Failed to set password for user [" + elasticUser + "].");
            }
        } catch (IOException e) {
            terminal.errorPrintln("");
            terminal.errorPrintln("Connection failure to: " + passwordChangeUrl.toString() + " failed: " + e.getMessage());
            terminal.errorPrintln("");
            terminal.errorPrintln(ExceptionsHelper.stackTrace(e));
            terminal.errorPrintln("");
            throw new UserException(ExitCodes.TEMP_FAILURE, "Failed to set password for user [" + elasticUser + "].", e);
        }
    }

    URL createURL(URL url, String path, String query) throws MalformedURLException, URISyntaxException {
        return new URL(url, (url.toURI().getPath() + path).replaceAll("/+", "/") + query);
    }

    URL checkClusterHealthUrl(CommandLineHttpClient client) throws MalformedURLException, URISyntaxException {
        return createURL(new URL(client.getDefaultURL()), "_cluster/health", "?pretty");
    }

    URL changeElasticUserPasswordUrl(CommandLineHttpClient client) throws MalformedURLException, URISyntaxException {
        return createURL(new URL(client.getDefaultURL()), "/_security/user/" + elasticUser + "/_password",
            "?pretty");
    }

    HttpResponse.HttpResponseBuilder responseBuilder(InputStream is) throws IOException {
        final HttpResponse.HttpResponseBuilder httpResponseBuilder = new HttpResponse.HttpResponseBuilder();
        final String responseBody = Streams.readFully(is).utf8ToString();
        httpResponseBuilder.withResponseBody(responseBody);
        return httpResponseBuilder;
    }

    char[] generatePassword(int passwordLength) {
        final char[] passwordChars = ("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789~!@#$%^&*-_=+?").toCharArray();
        char[] characters = new char[passwordLength];
        for (int i = 0; i < passwordLength; ++i) {
            characters[i] = passwordChars[new SecureRandom().nextInt(passwordChars.length)];
        }
        return characters;
    }

    private Map<String, String> getDecoded(String token) throws Exception {
        final String jsonString = new String(Base64.getDecoder().decode(token), StandardCharsets.UTF_8);
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE,
            jsonString)) {
            final Map<String, Object> info = parser.map();
            if (info == null) {
                throw new UserException(ExitCodes.DATA_ERROR,
                    "Unable to decode enrollment token.");
            }
            return info.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().toString()));
        }
    }

    // For testing
    OptionParser getParser() {
        return parser;
    }
}
