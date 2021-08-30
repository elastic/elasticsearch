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
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.security.tool.CommandLineHttpClient.createURL;
import static org.elasticsearch.xpack.security.tool.CommandUtils.generatePassword;

public class BootstrapPasswordAndEnrollmentTokenForInitialNode extends KeyStoreAwareCommand {
    private final CheckedFunction<Environment, EnrollmentTokenGenerator, Exception> createEnrollmentTokenFunction;
    private final Function<Environment, CommandLineHttpClient> clientFunction;
    private final CheckedFunction<Environment, KeyStoreWrapper, Exception> keyStoreFunction;
    private final OptionSpec<String> eofMarker;

    BootstrapPasswordAndEnrollmentTokenForInitialNode() {
        this(
            environment -> new CommandLineHttpClient(environment),
            environment -> KeyStoreWrapper.load(environment.configFile()),
            environment -> new EnrollmentTokenGenerator(environment)
        );
        // This "cli utility" must be invoked EXCLUSIVELY from the node startup script, where it is passed all the
        // node startup options unfiltered.
        // It cannot consume most of them, but it does need to inspect the `-E` ones
        parser.allowsUnrecognizedOptions();
    }

    BootstrapPasswordAndEnrollmentTokenForInitialNode(Function<Environment, CommandLineHttpClient> clientFunction,
                                                      CheckedFunction<Environment, KeyStoreWrapper, Exception> keyStoreFunction,
                                                      CheckedFunction<Environment, EnrollmentTokenGenerator, Exception>
                                                      createEnrollmentTokenFunction){
        super("Set elastic password and generate enrollment token for initial node");
        this.clientFunction = clientFunction;
        this.keyStoreFunction = keyStoreFunction;
        this.createEnrollmentTokenFunction = createEnrollmentTokenFunction;
        eofMarker = parser.accepts("eof-marker", "the last line of the printed text").withRequiredArg();
    }

    public static void main(String[] args) throws Exception {
        exit(new BootstrapPasswordAndEnrollmentTokenForInitialNode().main(args, Terminal.DEFAULT));
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws UserException {
        final SecureString keystorePassword = new SecureString(terminal.readSecret(""));
        final Environment secureEnvironment;
        final CommandLineHttpClient client;
        final EnrollmentTokenGenerator enrollmentTokenGenerator;
        try {
            secureEnvironment = readSecureSettings(env, keystorePassword);
            client = clientFunction.apply(secureEnvironment);
            enrollmentTokenGenerator = createEnrollmentTokenFunction.apply(secureEnvironment);
        } catch (Exception e) {
            throw new UserException(ExitCodes.CONFIG, null, e);
        }
        final SecureString bootstrapPassword = ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.get(secureEnvironment.settings());
        try {
            String output;
            client.checkClusterHealthWithRetriesWaitingForCluster(ElasticUser.NAME, bootstrapPassword, 30);
            final List<EnrollmentToken> enrollmentTokens = enrollmentTokenGenerator.createEnrollmentTokens(ElasticUser.NAME,
                    bootstrapPassword);
            if (enrollmentTokens == null || (enrollmentTokens.size() != 1 && enrollmentTokens.size() != 2)) {
                throw new IllegalStateException("Unexpected token generation " + enrollmentTokens);
            }
            final EnrollmentToken kibanaToken = enrollmentTokens.get(0);
            final EnrollmentToken nodeToken = enrollmentTokens.size() == 2 ? enrollmentTokens.get(1) : null;
            output = "The following information is only briefly displayed the first time the first node of a new cluster " +
                            "is started from a terminal." + System.lineSeparator();
            output += "Security is now enabled and has been automatically configured:" + System.lineSeparator();
            output += "HTTPS CA fingerprint: " + kibanaToken.getFingerprint() + System.lineSeparator();
            output += "Kibana enrollment token: " + kibanaToken.getEncoded() + System.lineSeparator();
            if (nodeToken != null) {
                output += "Node enrollment token: " + nodeToken.getEncoded() + System.lineSeparator();
            }
            if (ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.exists(secureEnvironment.settings()) == false) {
                output += "elastic user password: " + setElasticUserPassword(client, bootstrapPassword) + System.lineSeparator();
            }
            output += eofMarker.value(options);
            terminal.println(output);
        } catch (Exception e) {
            throw new UserException(ExitCodes.UNAVAILABLE, null, e);
        }
    }

    protected SecureString setElasticUserPassword(CommandLineHttpClient client, SecureString bootstrapPassword) throws Exception {
        final URL passwordSetUrl = setElasticUserPasswordUrl(client);
        final HttpResponse response;
        SecureString password = new SecureString(generatePassword(20));
        try {
            response = client.execute("POST", passwordSetUrl, ElasticUser.NAME, bootstrapPassword,
                () -> {
                    XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
                    xContentBuilder.startObject().field("password", password.toString()).endObject();
                    return Strings.toString(xContentBuilder);
                }, CommandLineHttpClient::responseBuilder);
            if (response.getHttpStatus() != HttpURLConnection.HTTP_OK) {
                throw new UserException(ExitCodes.UNAVAILABLE, null);
            }
        } catch (IOException e) {
            throw new UserException(ExitCodes.IO_ERROR, null, e);
        }
        return password;
    }

    Environment readSecureSettings(Environment env, SecureString keystorePassword) throws Exception {
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
        return createURL(new URL(client.getDefaultURL()), "_cluster/health/.security", "?pretty");
    }

    public static URL setElasticUserPasswordUrl(CommandLineHttpClient client) throws MalformedURLException, URISyntaxException {
        return createURL(new URL(client.getDefaultURL()), "/_security/user/" + ElasticUser.NAME + "/_password",
            "?pretty");
    }
}
