/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment.tool;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.enrollment.CreateEnrollmentToken;
import org.elasticsearch.xpack.security.tool.BaseClientAwareCommand;
import org.elasticsearch.xpack.security.tool.CommandLineHttpClient;
import org.elasticsearch.xpack.security.tool.HttpResponse;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.function.Function;

public class BootstrapPasswordAndEnrollmentForInitialNode extends BaseClientAwareCommand {
    private static final String elasticUser = ElasticUser.NAME;
    private KeyStoreWrapper keyStoreWrapper;
    private SecureString password;
    private String token;
    private String fingerprint;
    private String nodeToken;
    private final CheckedFunction<Environment, CreateEnrollmentToken, Exception> createEnrollmentTokenFunction;

    // Package-private for testing
    SecureString bootstrapPassword;
    SecureString credentialsPassword;

    SecureString getPassword() {
        return password;
    }
    String getKibanaToken() {
        return token;
    }
    String getFingerprint() {
        return fingerprint;
    }
    String getNodeToken() { return nodeToken; }
    CommandLineHttpClient getClient(Environment env) {
        return clientFunction.apply(env);
    }
    CreateEnrollmentToken getCreateEnrollmentToken(Environment env) throws Exception{
        return createEnrollmentTokenFunction.apply(env);
    }
    KeyStoreWrapper getKeyStoreWrapper(Environment env) throws Exception { return keyStoreFunction.apply(env); }
    OptionParser getParser() { return parser; }

    BootstrapPasswordAndEnrollmentForInitialNode() {
        this(
            environment -> new CommandLineHttpClient(environment),
            environment -> KeyStoreWrapper.load(environment.configFile()),
            environment -> new CreateEnrollmentToken(environment)
        );
    }

    BootstrapPasswordAndEnrollmentForInitialNode(Function<Environment, CommandLineHttpClient> clientFunction,
                                                 CheckedFunction<Environment, KeyStoreWrapper, Exception> keyStoreFunction,
                                                 CheckedFunction<Environment, CreateEnrollmentToken, Exception>
                                                     createEnrollmentTokenFunction){
        super(clientFunction, keyStoreFunction, "Set elastic password and generate enrollment token for initial node");
        this.createEnrollmentTokenFunction = createEnrollmentTokenFunction;
        parser.allowsUnrecognizedOptions();
    }

    public static void main(String[] args) throws Exception {
        exit(new BootstrapPasswordAndEnrollmentForInitialNode().main(args, Terminal.DEFAULT));
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        if (options.nonOptionArguments().contains("--explicitly-acknowledge-execution") == false) {
            throw new UserException(ExitCodes.CONFIG, "This command is not intended for end users");
        }
        if (env.settings().hasValue(XPackSettings.ENROLLMENT_ENABLED.getKey()) && false ==
            XPackSettings.ENROLLMENT_ENABLED.get(env.settings())) {
            throw new UserException(ExitCodes.NOOP, "Enrollment is explicitly disabled.");
        }
        final CommandLineHttpClient client = getClient(env);
        final CreateEnrollmentToken createEnrollmentToken = getCreateEnrollmentToken(env);
        keyStoreWrapper = getKeyStoreWrapper(env);
        ReadBootstrapPassword(env, terminal);
        checkClusterHealthWithRetries(client, terminal, elasticUser, credentialsPassword, 0, 5, false);
        if (Strings.isNullOrEmpty(bootstrapPassword.toString())) {
            changeElasticUserPassword(terminal, client);
        } else {
            password = bootstrapPassword;
        }
        token = createEnrollmentToken.createKibanaEnrollmentToken(elasticUser, password);
        fingerprint = createEnrollmentToken.getFingerprint();

        if (Strings.isNullOrEmpty(bootstrapPassword.toString())) {
            terminal.println("'elastic' user password: " + password);
        }
        terminal.println("CA fingerprint: " + fingerprint);
        terminal.println("Kibana enrollment token: " + token);
        if (options.nonOptionArguments().contains("--docker")) {
            nodeToken = createEnrollmentToken.createNodeEnrollmentToken(elasticUser, password);
            terminal.println("Node enrollment token: " + nodeToken);
        }
    }

    @Override
    public void close() {
        if (keyStoreWrapper != null) {
            keyStoreWrapper.close();
        }
        if (bootstrapPassword != null) {
            bootstrapPassword.close();
        }
        if (credentialsPassword != null) {
            credentialsPassword.close();
        }
    }

    protected void changeElasticUserPassword(Terminal terminal, CommandLineHttpClient client) throws Exception {
        final URL passwordChangeUrl = changeElasticUserPasswordUrl(client);
        final HttpResponse response;
        password = new SecureString(generatePassword(20));
        try {
            response = client.execute("POST", passwordChangeUrl, elasticUser, credentialsPassword,
                () -> {
                    XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
                    xContentBuilder.startObject().field("password", password).endObject();
                    return Strings.toString(xContentBuilder);
                }, this::responseBuilder);
            if (response.getHttpStatus() != HttpURLConnection.HTTP_OK) {
                terminal.errorPrintln("");
                terminal.errorPrintln(
                    "Unexpected response code [" + response.getHttpStatus() + "] from calling PUT " + passwordChangeUrl.toString());
                final String cause = CommandLineHttpClient.getErrorCause(response);
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

    void ReadBootstrapPassword (Environment env, Terminal terminal) throws Exception {
        decryptKeyStore(keyStoreWrapper, terminal);
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(env.settings(), true);
        if (settingsBuilder.getSecureSettings() == null) {
            settingsBuilder.setSecureSettings(keyStoreWrapper);
        }
        final Settings settings = settingsBuilder.build();
        bootstrapPassword = ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.get(settings);
        credentialsPassword = Strings.isNullOrEmpty(bootstrapPassword.toString()) ? KeyStoreWrapper.SEED_SETTING.get(settings) :
            bootstrapPassword;

        final Environment newEnv = new Environment(settings, env.configFile());
        Environment.assertEquivalent(newEnv, env);
    }

    URL checkClusterHealthUrl(CommandLineHttpClient client) throws MalformedURLException, URISyntaxException {
        return createURL(new URL(client.getDefaultURL()), "_cluster/health", "?pretty");
    }

    URL changeElasticUserPasswordUrl(CommandLineHttpClient client) throws MalformedURLException, URISyntaxException {
        return createURL(new URL(client.getDefaultURL()), "/_security/user/" + elasticUser + "/_password",
            "?pretty");
    }
}
