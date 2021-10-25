/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.esnative.tool;

import joptsimple.OptionSet;

import joptsimple.OptionSpecBuilder;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.core.security.user.APMSystemUser;
import org.elasticsearch.xpack.core.security.user.BeatsSystemUser;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaSystemUser;
import org.elasticsearch.xpack.core.security.user.LogstashSystemUser;
import org.elasticsearch.xpack.core.security.user.RemoteMonitoringUser;
import org.elasticsearch.xpack.security.tool.BaseRunAsSuperuserCommand;
import org.elasticsearch.xpack.core.security.CommandLineHttpClient;
import org.elasticsearch.xpack.core.security.HttpResponse;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.CommandLineHttpClient.createURL;
import static org.elasticsearch.xpack.security.tool.CommandUtils.generatePassword;

public class ResetBuiltinPasswordTool extends BaseRunAsSuperuserCommand {

    private final Function<Environment, CommandLineHttpClient> clientFunction;
    private final OptionSpecBuilder interactive;
    private final OptionSpecBuilder auto;
    private final OptionSpecBuilder batch;
    private final OptionSpecBuilder isElastic;
    private final OptionSpecBuilder isKibanaSystem;
    private final OptionSpecBuilder isLogstashSystem;
    private final OptionSpecBuilder isBeatsSystem;
    private final OptionSpecBuilder isApmSystem;
    private final OptionSpecBuilder isRemoteMonitoringUser;
    private String providedUsername;

    public ResetBuiltinPasswordTool() {
        this(CommandLineHttpClient::new, environment -> KeyStoreWrapper.load(environment.configFile()));
    }

    public static void main(String[] args) throws Exception {
        exit(new ResetBuiltinPasswordTool().main(args, Terminal.DEFAULT));
    }

    protected ResetBuiltinPasswordTool(
        Function<Environment, CommandLineHttpClient> clientFunction,
        CheckedFunction<Environment, KeyStoreWrapper, Exception> keyStoreFunction
    ) {

        super(clientFunction, keyStoreFunction, "Resets the password of a built-in user");
        parser.allowsUnrecognizedOptions();
        interactive = parser.acceptsAll(List.of("i", "interactive"));
        auto = parser.acceptsAll(List.of("a", "auto")); // default
        batch = parser.acceptsAll(List.of("b", "batch"));
        isElastic = parser.accepts(ElasticUser.NAME, "Resets the password of the elastic built-in user");
        isKibanaSystem = parser.accepts(KibanaSystemUser.NAME, "Resets the password of the kibana_system built-in user");
        isLogstashSystem = parser.accepts(LogstashSystemUser.NAME, "Resets the password of the logstash_system built-in user");
        isBeatsSystem = parser.accepts(BeatsSystemUser.NAME, "Resets the password of the beats_system built-in user");
        isApmSystem = parser.accepts(APMSystemUser.NAME, "Resets the password of the apm_system built-in user");
        isRemoteMonitoringUser = parser.accepts(
            RemoteMonitoringUser.NAME,
            "Resets the password of the remote_monitoring_user built-in user"
        );
        parser.mutuallyExclusive(isElastic, isKibanaSystem, isLogstashSystem, isBeatsSystem, isApmSystem, isRemoteMonitoringUser);
        this.clientFunction = clientFunction;
    }

    @Override
    protected void executeCommand(Terminal terminal, OptionSet options, Environment env, String username, SecureString password)
        throws Exception {
        final SecureString builtinUserPassword;
        if (options.has(interactive)) {
            if (options.has(batch) == false) {
                terminal.println("This tool will reset the password of the [" + providedUsername + "] user.");
                terminal.println("You will be prompted to enter the password.");
                boolean shouldContinue = terminal.promptYesNo("Please confirm that you would like to continue", false);
                terminal.println("\n");
                if (shouldContinue == false) {
                    throw new UserException(ExitCodes.OK, "User cancelled operation");
                }
            }
            builtinUserPassword = promptForPassword(terminal, providedUsername);
        } else {
            if (options.has(batch) == false) {
                terminal.println("This tool will reset the password of the [" + providedUsername + "] user to an autogenerated value.");
                terminal.println("The password will be printed in the console.");
                boolean shouldContinue = terminal.promptYesNo("Please confirm that you would like to continue", false);
                terminal.println("\n");
                if (shouldContinue == false) {
                    throw new UserException(ExitCodes.OK, "User cancelled operation");
                }
            }
            builtinUserPassword = new SecureString(generatePassword(20));
        }
        try {
            final CommandLineHttpClient client = clientFunction.apply(env);
            final URL changePasswordUrl = createURL(
                new URL(client.getDefaultURL()),
                "_security/user/" + providedUsername + "/_password",
                "?pretty"
            );
            final HttpResponse httpResponse = client.execute(
                "POST",
                changePasswordUrl,
                username,
                password,
                () -> requestBodySupplier(builtinUserPassword),
                CommandLineHttpClient::responseBuilder
            );
            final int responseStatus = httpResponse.getHttpStatus();
            if (httpResponse.getHttpStatus() != HttpURLConnection.HTTP_OK) {
                throw new UserException(ExitCodes.TEMP_FAILURE,
                    "Failed to reset password for the [" + providedUsername + "] user. Unexpected http status [" + responseStatus + "]");
            } else {
                if (options.has(interactive)) {
                    terminal.println("Password for the [" + providedUsername + "] user successfully reset.");
                } else {
                    terminal.println("Password for the [" + providedUsername + "] user successfully reset.");
                    terminal.print(Terminal.Verbosity.NORMAL,"New value: ");
                    terminal.println(Terminal.Verbosity.SILENT, builtinUserPassword.toString());
                }
            }
        } catch (Exception e) {
            throw new UserException(ExitCodes.TEMP_FAILURE, "Failed to reset password for the [" + providedUsername + "] user", e);
        } finally {
            builtinUserPassword.close();
        }
    }

    private SecureString promptForPassword(Terminal terminal, String providedUsername) {
        while (true) {
            SecureString password1 = new SecureString(terminal.readSecret("Enter password for [" + providedUsername + "]: "));
            Validation.Error err = Validation.Users.validatePassword(password1);
            if (err != null) {
                terminal.errorPrintln(err.toString());
                terminal.errorPrintln("Try again.");
                password1.close();
                continue;
            }
            try (SecureString password2 = new SecureString(terminal.readSecret("Re-enter password for [" + providedUsername + "]: "))) {
                if (password1.equals(password2) == false) {
                    terminal.errorPrintln("Passwords do not match.");
                    terminal.errorPrintln("Try again.");
                    password1.close();
                    continue;
                }
            }
            return password1;
        }
    }

    private String requestBodySupplier(SecureString pwd) throws Exception {
        XContentBuilder xContentBuilder = JsonXContent.contentBuilder();
        xContentBuilder.startObject().field("password", pwd.toString()).endObject();
        return Strings.toString(xContentBuilder);
    }

    @Override
    protected void validate(Terminal terminal, OptionSet options, Environment env) throws Exception {
        if ((options.has("i") || options.has("interactive")) && (options.has("a") || options.has("auto"))) {
            throw new UserException(ExitCodes.USAGE, "You can only run the tool in one of [auto] or [interactive] modes");
        }
        if (options.has(isElastic)) {
            providedUsername = "elastic";
        } else if (options.has(isKibanaSystem)) {
            providedUsername = "kibana_system";
        } else if (options.has(isLogstashSystem)) {
            providedUsername = "logstash_system";
        } else if (options.has(isApmSystem)) {
            providedUsername = "apm_system";
        } else if (options.has(isBeatsSystem)) {
            providedUsername = "beats_system";
        } else if (options.has(isRemoteMonitoringUser)) {
            providedUsername = "remote_monitoring_user";
        } else {
            throw new UserException(
                ExitCodes.USAGE,
                "You need to specify one of the following parameters: ["
                    + List.of(
                        ElasticUser.NAME,
                        APMSystemUser.NAME,
                        KibanaSystemUser.NAME,
                        LogstashSystemUser.NAME,
                        BeatsSystemUser.NAME,
                        RemoteMonitoringUser.NAME
                    ).stream().map(n -> "--" + n).collect(Collectors.joining(","))
                    + "]."
            );
        }
    }

}
