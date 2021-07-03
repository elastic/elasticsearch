/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.tool;

import joptsimple.OptionSet;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.KeyStoreAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.authc.file.FileUserPasswdStore;
import org.elasticsearch.xpack.security.authc.file.FileUserRolesStore;
import org.elasticsearch.xpack.security.support.FileAttributesChecker;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * A {@link KeyStoreAwareCommand} that can be extended fpr any CLI tool that needs to allow a local user with
 * filesystem write access to perform actions on the node as a superuser. It leverages temporary file realm users
 * with a `superuser` role.
 */
public abstract class BaseRunAsSuperuserCommand extends KeyStoreAwareCommand {

    private static final String[] ROLES = new String[] { "superuser" };
    private static final int PASSWORD_LENGTH = 14;

    private final Function<Environment, CommandLineHttpClient> clientFunction;
    private final CheckedFunction<Environment, KeyStoreWrapper, Exception> keyStoreFunction;
    private SecureString password;
    private String username;
    final SecureRandom secureRandom = new SecureRandom();

    public BaseRunAsSuperuserCommand(
        Function<Environment, CommandLineHttpClient> clientFunction,
        CheckedFunction<Environment, KeyStoreWrapper, Exception> keyStoreFunction
    ) {
        super("description");
        this.clientFunction = clientFunction;
        this.keyStoreFunction = keyStoreFunction;
    }

    @Override
    public void close() {
        if (password != null) {
            password.close();
        }
    }

    @Override
    protected final void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        validate(terminal, options, env);
        KeyStoreWrapper keyStoreWrapper = keyStoreFunction.apply(env);
        final Environment newEnv;
        final Settings settings;
        if (keyStoreWrapper != null) {
            decryptKeyStore(keyStoreWrapper, terminal);
            Settings.Builder settingsBuilder = Settings.builder();
            settingsBuilder.put(env.settings(), true);
            if (settingsBuilder.getSecureSettings() == null) {
                settingsBuilder.setSecureSettings(keyStoreWrapper);
            }
            settings = settingsBuilder.build();
            newEnv = new Environment(settings, env.configFile());
        } else {
            newEnv = env;
            settings = env.settings();
        }

        ensureFileRealmEnabled(settings);
        try {
            final Hasher hasher = Hasher.resolve(XPackSettings.PASSWORD_HASHING_ALGORITHM.get(settings));
            password = new SecureString(generatePassword(PASSWORD_LENGTH));
            username = generateUsername();

            final Path passwordFile = FileUserPasswdStore.resolveFile(newEnv);
            final Path rolesFile = FileUserRolesStore.resolveFile(newEnv);
            FileAttributesChecker attributesChecker = new FileAttributesChecker(passwordFile, rolesFile);

            Map<String, char[]> users = FileUserPasswdStore.parseFile(passwordFile, null, settings);
            if (users == null) {
                throw new IllegalStateException("File realm configuration file [" + passwordFile + "] is missing");
            }
            users = new HashMap<>(users);
            users.put(username, hasher.hash(password));
            FileUserPasswdStore.writeFile(users, passwordFile);

            Map<String, String[]> userRoles = FileUserRolesStore.parseFile(rolesFile, null);
            if (userRoles == null) {
                throw new IllegalStateException("File realm configuration file [" + rolesFile + "] is missing");
            }
            userRoles = new HashMap<>(userRoles);
            userRoles.put(username, ROLES);
            FileUserRolesStore.writeFile(userRoles, rolesFile);

            attributesChecker.check(terminal);
            checkClusterHealthWithRetries(newEnv, terminal, 5);
            executeCommand(terminal, options, newEnv);
        } catch (Exception e) {
            int exitCode;
            if (e instanceof UserException) {
                exitCode = ((UserException) e).exitCode;
            } else {
                exitCode = ExitCodes.DATA_ERROR;
            }
            throw new UserException(exitCode, e.getMessage());
        } finally {
            cleanup(terminal, newEnv);
        }
    }

    /**
     * Removes temporary file realm user from users and roles file
     */
    protected void cleanup(Terminal terminal, Environment env) throws Exception {
        final Path passwordFile = FileUserPasswdStore.resolveFile(env);
        final Path rolesFile = FileUserRolesStore.resolveFile(env);
        FileAttributesChecker attributesChecker = new FileAttributesChecker(passwordFile, rolesFile);

        Map<String, char[]> users = FileUserPasswdStore.parseFile(passwordFile, null, env.settings());
        if (users == null) {
            throw new UserException(ExitCodes.CONFIG, "File realm configuration file [" + passwordFile + "] is missing");
        }
        users = new HashMap<>(users);
        char[] passwd = users.remove(username);
        if (passwd != null) {
            // No need to overwrite, if the user was already removed
            FileUserPasswdStore.writeFile(users, passwordFile);
            Arrays.fill(passwd, '\0');
        }

        Map<String, String[]> userRoles = FileUserRolesStore.parseFile(rolesFile, null);
        if (userRoles == null) {
            throw new UserException(ExitCodes.CONFIG, "File realm configuration file [" + rolesFile + "] is missing");
        }
        userRoles = new HashMap<>(userRoles);
        String[] roles = userRoles.remove(username);
        if (roles != null) {
            // No need to overwrite, if the user was already removed
            FileUserRolesStore.writeFile(userRoles, rolesFile);
        }
        attributesChecker.check(terminal);
    }

    protected SecureString getPassword() {
        return password;
    }

    protected String getUsername() {
        return username;
    }

    private void ensureFileRealmEnabled(Settings settings) throws Exception {
        Map<String, Settings> fileRealmSettings = settings.getGroups("xpack.security.authc.realms.file");
        if (fileRealmSettings.size() > 1) {
            throw new UserException(
                ExitCodes.CONFIG,
                "Multiple file realms are configured. "
                    + "[file] is an internal realm type and therefore there can only be one such realm configured"
            );
        } else if (fileRealmSettings.size() == 1
            && fileRealmSettings.entrySet().stream().anyMatch(s -> s.getValue().get("enabled").equals("false"))) {
                throw new UserException(ExitCodes.CONFIG, "File realm must be enabled");
            }
        // Else it's either explicitly enabled, or not defined in the settings so it is implicitly enabled.
    }

    /**
     * Checks that we can connect to the cluster and that the cluster health is not RED. It optionally handles
     * retries as the file realm might not have reloaded the users file yet in order to authenticate our
     * newly created file realm user.
     */
    private void checkClusterHealthWithRetries(Environment env, Terminal terminal, int retries) throws Exception {
        CommandLineHttpClient client = clientFunction.apply(env);
        final URL clusterHealthUrl = createURL(new URL(client.getDefaultURL()), "_cluster/health", "?pretty");
        final HttpResponse response;
        try {
            response = client.execute("GET", clusterHealthUrl, username, password, () -> null, this::responseBuilder);
        } catch (Exception e) {
            throw new UserException(ExitCodes.UNAVAILABLE, "Failed to determine the health of the cluster. ", e);
        }
        final int responseStatus = response.getHttpStatus();
        if (responseStatus != HttpURLConnection.HTTP_OK) {
            if (responseStatus == HttpURLConnection.HTTP_UNAUTHORIZED && retries > 0) {
                terminal.println(
                    Terminal.Verbosity.VERBOSE,
                    "Unexpected http status [401] while attempting to determine cluster health. Will retry at most "
                        + retries
                        + " more times."
                );
                Thread.sleep(1000);
                retries -= 1;
                checkClusterHealthWithRetries(env, terminal, retries);
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
                    "Failed to determine the health of the cluster. Cluster health API did not return a status value"
                );
            } else if ("red".equalsIgnoreCase(clusterStatus)) {
                throw new UserException(ExitCodes.UNAVAILABLE, "Cluster health is currently RED.");
            }
            // else it is yellow or green so we can continue
        }
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
            characters[i] = passwordChars[secureRandom.nextInt(passwordChars.length)];
        }
        return characters;
    }

    private String generateUsername() {
        final char[] usernameChars = ("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789").toCharArray();
        int usernameLength = 8;
        char[] characters = new char[usernameLength];
        for (int i = 0; i < usernameLength; ++i) {
            characters[i] = usernameChars[secureRandom.nextInt(usernameChars.length)];
        }
        return "enrollment_autogenerated_" + new String(characters);
    }

    URL createURL(URL url, String path, String query) throws MalformedURLException, URISyntaxException {
        return new URL(url, (url.toURI().getPath() + path).replaceAll("/+", "/") + query);
    }

    /**
     * This is called after we have created a temporary superuser in the file realm and verified that its
     * credentials work. The username and password αρε available to classes extending {@link BaseRunAsSuperuserCommand}
     * using {@link BaseRunAsSuperuserCommand#getPassword}
     */
    protected abstract void executeCommand(Terminal terminal, OptionSet options, Environment env) throws Exception;

    /**
     * This method is called before we attempt to crete a temporary superuser in the file realm. Commands that
     * implement {@link BaseRunAsSuperuserCommand} can do preflight checks such as parsing and validating options without
     * the need to go through the process of attempting to create and remove the temporary user unnecessarily.
     */
    protected abstract void validate(Terminal terminal, OptionSet options, Environment env) throws Exception;
}
