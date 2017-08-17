/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative.tool;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.MultiCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.security.user.ElasticUser;
import org.elasticsearch.xpack.security.user.KibanaUser;
import org.elasticsearch.xpack.security.user.LogstashSystemUser;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * A tool to set passwords of internal users. It first sets the elastic user password. After the elastic user
 * password is set, it will set the remaining user passwords. This tool will only work if the passwords have
 * not already been set by something else.
 */
public class SetupPasswordTool extends MultiCommand {

    private static final char[] CHARS = ("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789" +
            "~!@#$%^&*-_=+?").toCharArray();
    private static final String[] USERS = new String[]{ElasticUser.NAME, KibanaUser.NAME, LogstashSystemUser.NAME};

    private final Function<Environment, CommandLineHttpClient> clientFunction;
    private final CheckedFunction<Environment, KeyStoreWrapper, Exception> keyStoreFunction;
    private CommandLineHttpClient client;

    SetupPasswordTool() {
        this((environment) -> new CommandLineHttpClient(environment.settings(), environment),
                (environment) -> {
                    KeyStoreWrapper keyStoreWrapper = KeyStoreWrapper.load(environment.configFile());
                    if (keyStoreWrapper == null) {
                        throw new UserException(ExitCodes.CONFIG, "Keystore does not exist");
                    }
                    return keyStoreWrapper;
                });
    }

    SetupPasswordTool(Function<Environment, CommandLineHttpClient> clientFunction,
                      CheckedFunction<Environment, KeyStoreWrapper, Exception> keyStoreFunction) {
        super("Sets the passwords for reserved users");
        subcommands.put("auto", newAutoSetup());
        subcommands.put("interactive", newInteractiveSetup());
        this.clientFunction = clientFunction;
        this.keyStoreFunction = keyStoreFunction;
    }

    protected AutoSetup newAutoSetup() {
        return new AutoSetup();
    }

    protected InteractiveSetup newInteractiveSetup() {
        return new InteractiveSetup();
    }

    public static void main(String[] args) throws Exception {
        exit(new SetupPasswordTool().main(args, Terminal.DEFAULT));
    }

    // Visible for testing
    OptionParser getParser() {
        return this.parser;
    }

    /**
     * This class sets the passwords using automatically generated random passwords. The passwords will be
     * printed to the console.
     */
    class AutoSetup extends SetupCommand {

        AutoSetup() {
            super("Uses randomly generated passwords");
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
            setupOptions(options, env);

            if (shouldPrompt) {
                terminal.println("Initiating the setup of reserved user " + Arrays.toString(USERS) + "  passwords.");
                terminal.println("The passwords will be randomly generated and printed to the console.");
                boolean shouldContinue = terminal.promptYesNo("Please confirm that you would like to continue", false);
                terminal.println("\n");
                if (shouldContinue == false) {
                    throw new UserException(ExitCodes.OK, "User cancelled operation");
                }
            }

            SecureRandom secureRandom = new SecureRandom();
            changePasswords(terminal, (user) -> generatePassword(secureRandom, user),
                    (user, password) -> changedPasswordCallback(terminal, user, password));
        }

        private SecureString generatePassword(SecureRandom secureRandom, String user) {
            int passwordLength = 20; // Generate 20 character passwords
            char[] characters = new char[passwordLength];
            for (int i = 0; i < passwordLength; ++i) {
                characters[i] = CHARS[secureRandom.nextInt(CHARS.length)];
            }
            return new SecureString(characters);
        }

        private void changedPasswordCallback(Terminal terminal, String user, SecureString password) {
            terminal.println("Changed password for user " + user + "\n" + "PASSWORD " + user + " = " + password + "\n");
        }

    }

    /**
     * This class sets the passwords using password entered manually by the user from the console.
     */
    class InteractiveSetup extends SetupCommand {

        InteractiveSetup() {
            super("Uses passwords entered by a user");
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
            setupOptions(options, env);

            if (shouldPrompt) {
                terminal.println("Initiating the setup of reserved user " + Arrays.toString(USERS) + "  passwords.");
                terminal.println("You will be prompted to enter passwords as the process progresses.");
                boolean shouldContinue = terminal.promptYesNo("Please confirm that you would like to continue", false);
                terminal.println("\n");
                if (shouldContinue == false) {
                    throw new UserException(ExitCodes.OK, "User cancelled operation");
                }
            }

            changePasswords(terminal, user -> promptForPassword(terminal, user),
                    (user, password) -> changedPasswordCallback(terminal, user, password));
        }

        private SecureString promptForPassword(Terminal terminal, String user) throws UserException {
            SecureString password1 = new SecureString(terminal.readSecret("Enter password for [" + user + "]: "));
            try (SecureString password2 = new SecureString(terminal.readSecret("Reenter password for [" + user + "]: "))) {
                if (password1.equals(password2) == false) {
                    password1.close();
                    throw new UserException(ExitCodes.USAGE, "Passwords for user [" + user + "] do not match");
                }
            }
            return password1;
        }

        private void changedPasswordCallback(Terminal terminal, String user, SecureString password) {
            terminal.println("Changed password for user " + user + "\n");
        }
    }

    /**
     * An abstract class that provides functionality common to both the auto and interactive setup modes.
     */
    private abstract class SetupCommand extends EnvironmentAwareCommand {

        boolean shouldPrompt;

        private OptionSpec<String> urlOption;
        private OptionSpec<String> noPromptOption;

        private String elasticUser = ElasticUser.NAME;
        private SecureString elasticUserPassword;
        private String url;

        SetupCommand(String description) {
            super(description);
            setParser();
        }

        void setupOptions(OptionSet options, Environment env) throws Exception {
            client = clientFunction.apply(env);
            try (KeyStoreWrapper keyStore = keyStoreFunction.apply(env)) {
                String providedUrl = urlOption.value(options);
                url = providedUrl == null ? client.getDefaultURL() : providedUrl;
                setShouldPrompt(options);

                // TODO: We currently do  not support keystore passwords
                keyStore.decrypt(new char[0]);
                Settings build = Settings.builder().setSecureSettings(keyStore).build();
                elasticUserPassword = ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.get(build);
            }
        }

        private void setParser() {
            urlOption = parser.acceptsAll(Arrays.asList("u", "url"), "The url for the change password request").withOptionalArg();
            noPromptOption = parser.acceptsAll(Arrays.asList("b", "batch"), "Whether the user should be prompted to initiate the "
                    + "change password process").withOptionalArg();
        }

        private void setShouldPrompt(OptionSet options) {
            String optionalNoPrompt = noPromptOption.value(options);
            if (options.has(noPromptOption)) {
                shouldPrompt = optionalNoPrompt != null && Booleans.parseBoolean(optionalNoPrompt) == false;
            } else {
                shouldPrompt = true;
            }
        }

        void changePasswords(Terminal terminal, CheckedFunction<String, SecureString, UserException> passwordFn,
                             BiConsumer<String, SecureString> callback) throws Exception {
            for (String user : USERS) {
                changePassword(terminal, url, user, passwordFn, callback);
            }
        }

        private void changePassword(Terminal terminal, String url, String user,
                                    CheckedFunction<String, SecureString, UserException> passwordFn,
                                    BiConsumer<String, SecureString> callback) throws Exception {
            boolean isSuperUser = user.equals(elasticUser);
            SecureString password = passwordFn.apply(user);

            try {
                String route = url + "/_xpack/security/user/" + user + "/_password";
                client.postURL("PUT", route, elasticUser, elasticUserPassword, buildPayload(password));
                callback.accept(user, password);
                if (isSuperUser) {
                    elasticUserPassword = password;
                }
            } catch (Exception e) {
                terminal.println("Exception making http rest request for user [" + user + "]");
                throw e;
            } finally {
                // We do not close the password if it is the super user as we are going to use the super user
                // password in the followup requests to change other user passwords
                if (isSuperUser == false) {
                    password.close();
                }
            }
        }

        private String buildPayload(SecureString password) throws IOException {
            XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
            xContentBuilder.startObject().field("password", password.toString()).endObject();
            return xContentBuilder.string();
        }
    }
}
