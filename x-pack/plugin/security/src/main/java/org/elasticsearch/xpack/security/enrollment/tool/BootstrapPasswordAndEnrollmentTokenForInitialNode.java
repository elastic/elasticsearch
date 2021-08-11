/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.enrollment.tool;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.KeyStoreAwareCommand;
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
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.enrollment.EnrollmentToken;
import org.elasticsearch.xpack.security.enrollment.EnrollmentTokenGenerator;
import org.elasticsearch.xpack.security.tool.CommandLineHttpClient;
import org.elasticsearch.xpack.security.tool.HttpResponse;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.SecureRandom;
import java.util.function.Function;

import static org.elasticsearch.xpack.security.tool.CommandLineHttpClient.createURL;

public class BootstrapPasswordAndEnrollmentTokenForInitialNode extends KeyStoreAwareCommand {
    private final CheckedFunction<Environment, EnrollmentTokenGenerator, Exception> createEnrollmentTokenFunction;
    private final Function<Environment, CommandLineHttpClient> clientFunction;
    private final CheckedFunction<Environment, KeyStoreWrapper, Exception> keyStoreFunction;
    private final OptionSpec<Void> docker;
    private final SecureRandom secureRandom = new SecureRandom();

    BootstrapPasswordAndEnrollmentTokenForInitialNode() {
        this(
            environment -> new CommandLineHttpClient(environment),
            environment -> KeyStoreWrapper.load(environment.configFile()),
            environment -> new EnrollmentTokenGenerator(environment)
        );
    }

    BootstrapPasswordAndEnrollmentTokenForInitialNode(Function<Environment, CommandLineHttpClient> clientFunction,
                                                      CheckedFunction<Environment, KeyStoreWrapper, Exception> keyStoreFunction,
                                                      CheckedFunction<Environment, EnrollmentTokenGenerator, Exception>
                                                     createEnrollmentTokenFunction){
        super("Set elastic password and generate enrollment token for initial node");
        this.clientFunction = clientFunction;
        this.keyStoreFunction = keyStoreFunction;
        this.createEnrollmentTokenFunction = createEnrollmentTokenFunction;
        docker = parser.accepts("include-node-enrollment-token", "determine that we are running in docker");
    }

    public static void main(String[] args) throws Exception {
        exit(new BootstrapPasswordAndEnrollmentTokenForInitialNode().main(args, Terminal.DEFAULT));
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        final char[] keyStorePassword;
        try {
            keyStorePassword = terminal.readSecret("");
        } catch (Exception e) {
            throw new UserException(ExitCodes.USAGE, null);
        }

        final SecureString keystorePassword = new SecureString(keyStorePassword);
        final CommandLineHttpClient client = clientFunction.apply(env);
        final EnrollmentTokenGenerator enrollmentTokenGenerator = createEnrollmentTokenFunction.apply(env);
        final Environment newEnvironment = readBootstrapPassword(env, keystorePassword);
        final SecureString bootstrapPassword = ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.get(newEnvironment.settings());
        String output;
        try {
            client.checkClusterHealthWithRetriesWaitingForCluster(ElasticUser.NAME, bootstrapPassword, 5, false);
            final EnrollmentToken kibanaToken = enrollmentTokenGenerator.createKibanaEnrollmentToken(ElasticUser.NAME, bootstrapPassword);
            output = "Kibana enrollment token: " + kibanaToken.encode() + "\n";
            output = output.concat("CA fingerprint: " + kibanaToken.getFingerprint() + "\n");
            if (options.has(docker)) {
                final EnrollmentToken nodeToken = enrollmentTokenGenerator.createNodeEnrollmentToken(ElasticUser.NAME, bootstrapPassword);
                output = output.concat("Node enrollment token: " + nodeToken.encode() + "\n");
            }
        } catch (Exception e) {
            throw new UserException(ExitCodes.UNAVAILABLE, null);
        }
        if (ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.exists(newEnvironment.settings()) == false) {
            output = output.concat("elastic user password: " + setElasticUserPassword(client, bootstrapPassword));
        }
        terminal.println(output);
    }

    protected SecureString setElasticUserPassword(CommandLineHttpClient client, SecureString bootstrapPassword) throws Exception {
        final URL passwordSetUrl = setElasticUserPasswordUrl(client);
        final HttpResponse response;
        SecureString password = new SecureString(generatePassword(20));
        try {
            response = client.execute("POST", passwordSetUrl, ElasticUser.NAME, bootstrapPassword,
                () -> {
                    XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
                    xContentBuilder.startObject().field("password", password).endObject();
                    return Strings.toString(xContentBuilder);
                }, CommandLineHttpClient::responseBuilder);
            if (response.getHttpStatus() != HttpURLConnection.HTTP_OK) {
                throw new UserException(ExitCodes.UNAVAILABLE, null);
            }
        } catch (IOException e) {
            throw new UserException(ExitCodes.IO_ERROR, null);
        }
        return password;
    }

    Environment readBootstrapPassword(Environment env, SecureString keystorePassword) throws Exception {
        final KeyStoreWrapper keyStoreWrapper = keyStoreFunction.apply(env);
        keyStoreWrapper.decrypt(keystorePassword.getChars());
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(env.settings(), true);
        if (settingsBuilder.getSecureSettings() == null) {
            settingsBuilder.setSecureSettings(keyStoreWrapper);
        }
        final Settings settings = settingsBuilder.build();
        return new Environment(settings, env.configFile());
    }

    public static URL checkClusterHealthUrl(CommandLineHttpClient client) throws MalformedURLException, URISyntaxException {
        return createURL(new URL(client.getDefaultURL()), "_cluster/health", "?pretty");
    }

    public static URL setElasticUserPasswordUrl(CommandLineHttpClient client) throws MalformedURLException, URISyntaxException {
        return createURL(new URL(client.getDefaultURL()), "/_security/user/" + ElasticUser.NAME + "/_password",
            "?pretty");
    }

    protected char[] generatePassword(int passwordLength) {
        final char[] passwordChars = ("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789~!@#$%^&*-_=+?").toCharArray();
        char[] characters = new char[passwordLength];
        for (int i = 0; i < passwordLength; ++i) {
            characters[i] = passwordChars[secureRandom.nextInt(passwordChars.length)];
        }
        return characters;
    }
}
