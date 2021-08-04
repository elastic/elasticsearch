/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.tool;

import org.elasticsearch.Version;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.KeyStoreAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.SecureRandom;
import java.util.Objects;
import java.util.function.Function;

/**
 * A {@link KeyStoreAwareCommand} that can be extended for any CLI tool that needs use a http client.
 */
public abstract class BaseClientAwareCommand extends KeyStoreAwareCommand {
    protected final Function<Environment, CommandLineHttpClient> clientFunction;
    protected final CheckedFunction<Environment, KeyStoreWrapper, Exception> keyStoreFunction;
    final SecureRandom secureRandom = new SecureRandom();

    public BaseClientAwareCommand(
        Function<Environment, CommandLineHttpClient> clientFunction,
        CheckedFunction<Environment, KeyStoreWrapper, Exception> keyStoreFunction,
        String description
    ) {
        super(description);
        this.clientFunction = clientFunction;
        this.keyStoreFunction = keyStoreFunction;
    }

    /**
     * Checks that we can connect to the cluster and that the cluster health is not RED. It optionally handles
     * retries as the file realm might not have reloaded the users file yet in order to authenticate our
     * newly created file realm user.
     */
    protected void checkClusterHealthWithRetries(CommandLineHttpClient client, Terminal terminal, String username, SecureString password,
                                                 int retriesWaitingForUser, int retriesWaitingForNode, boolean force) throws Exception {
        final URL clusterHealthUrl = createURL(new URL(client.getDefaultURL()), "_cluster/health", "?pretty");
        HttpResponse response = null;
        try {
            response = client.execute("GET", clusterHealthUrl, username, password, () -> null, this::responseBuilder);
        } catch (Exception e) {
            if (retriesWaitingForNode > 0) {
                terminal.println(
                    Terminal.Verbosity.VERBOSE,
                    "Failed to determine the health of the cluster. Will retry at most " + retriesWaitingForNode + " more times."
                );
                Thread.sleep(1000);
                retriesWaitingForNode -= 1;
                checkClusterHealthWithRetries(client, terminal, username, password, retriesWaitingForUser, retriesWaitingForNode, force);
            } else {
                throw new UserException(ExitCodes.UNAVAILABLE, "Failed to determine the health of the cluster. ", e);
            }
        }
        final int responseStatus = Objects.requireNonNull(response).getHttpStatus();
        if (responseStatus != HttpURLConnection.HTTP_OK) {
            // We try to write the roles file first and then the users one, but theoretically we could have loaded the users
            // before we have actually loaded the roles so we also retry on 403 ( temp user is found but has no roles )
            if ((responseStatus == HttpURLConnection.HTTP_UNAUTHORIZED || responseStatus == HttpURLConnection.HTTP_FORBIDDEN)
                && retriesWaitingForUser > 0) {
                terminal.println(
                    Terminal.Verbosity.VERBOSE,
                    "Unexpected http status [" + responseStatus + "] while attempting to determine cluster health. Will retry at most "
                        + retriesWaitingForUser
                        + " more times."
                );
                Thread.sleep(1000);
                retriesWaitingForUser -= 1;
                checkClusterHealthWithRetries(client, terminal, username, password, retriesWaitingForUser, retriesWaitingForNode, force);
            } else {
                throw new UserException(
                    ExitCodes.DATA_ERROR,
                    "Failed to determine the health of the cluster. Unexpected http status [" + responseStatus + "]"
                );
            }
        } else {
            final String clusterStatus = Objects.toString(response.getResponseBody().get("status"), "");
            if (clusterStatus.isEmpty()) {
                throw new UserException(
                    ExitCodes.DATA_ERROR,
                    "Failed to determine the health of the cluster. Cluster health API did not return a status value."
                );
            } else if ("red".equalsIgnoreCase(clusterStatus) && force == false) {
                if (retriesWaitingForNode > 0) {
                    terminal.println(
                        Terminal.Verbosity.VERBOSE,
                        "Cluster health is currently RED. Will retry at most " + retriesWaitingForNode + " more times."
                    );
                    Thread.sleep(1000);
                    retriesWaitingForNode -= 1;
                    checkClusterHealthWithRetries(client, terminal, username, password, retriesWaitingForUser, retriesWaitingForNode,
                        force);
                } else {
                    terminal.errorPrintln("Failed to determine the health of the cluster. Cluster health is currently RED.");
                    terminal.errorPrintln("This means that some cluster data is unavailable and your cluster is not fully functional.");
                    terminal.errorPrintln("The cluster logs (https://www.elastic.co/guide/en/elasticsearch/reference/"
                        + Version.CURRENT.major + "." + Version.CURRENT.minor + "/logging.html)"
                        + " might contain information/indications for the underlying cause");
                    terminal.errorPrintln(
                        "It is recommended that you resolve the issues with your cluster before continuing");
                    terminal.errorPrintln("It is very likely that the command will fail when run against an unhealthy cluster.");
                    terminal.errorPrintln("");
                    terminal.errorPrintln("If you still want to attempt to execute this command against an unhealthy cluster," +
                        " you can pass the `-f` parameter.");
                    throw new UserException(ExitCodes.UNAVAILABLE,
                        "Failed to determine the health of the cluster. Cluster health is currently RED.");
                }
            }
            // else it is yellow or green so we can continue
        }
    }

    protected HttpResponse.HttpResponseBuilder responseBuilder(InputStream is) throws IOException {
        final HttpResponse.HttpResponseBuilder httpResponseBuilder = new HttpResponse.HttpResponseBuilder();
        final String responseBody = Streams.readFully(is).utf8ToString();
        httpResponseBuilder.withResponseBody(responseBody);
        return httpResponseBuilder;
    }

    protected URL createURL(URL url, String path, String query) throws MalformedURLException, URISyntaxException {
        return new URL(url, (url.toURI().getPath() + path).replaceAll("/+", "/") + query);
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
