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
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.CommandLineHttpClient;
import org.elasticsearch.xpack.security.enrollment.ExternalEnrollmentTokenGenerator;
import org.elasticsearch.xpack.security.tool.BaseRunAsSuperuserCommand;

import java.net.URL;
import java.util.List;
import java.util.function.Function;

public class CreateEnrollmentTokenTool extends BaseRunAsSuperuserCommand {

    private final OptionSpec<String> scope;
    private final Function<Environment, CommandLineHttpClient> clientFunction;
    private final CheckedFunction<Environment, ExternalEnrollmentTokenGenerator, Exception> createEnrollmentTokenFunction;
    static final List<String> ALLOWED_SCOPES = List.of("node", "kibana");

    CreateEnrollmentTokenTool() {

        this(
            environment -> new CommandLineHttpClient(environment),
            environment -> KeyStoreWrapper.load(environment.configFile()),
            environment -> new ExternalEnrollmentTokenGenerator(environment)
        );
    }

    CreateEnrollmentTokenTool(
        Function<Environment, CommandLineHttpClient> clientFunction,
        CheckedFunction<Environment, KeyStoreWrapper, Exception> keyStoreFunction,
        CheckedFunction<Environment, ExternalEnrollmentTokenGenerator, Exception> createEnrollmentTokenFunction
    ) {
        super(clientFunction, keyStoreFunction, "Creates enrollment tokens for elasticsearch nodes and kibana instances");
        this.createEnrollmentTokenFunction = createEnrollmentTokenFunction;
        this.clientFunction = clientFunction;
        scope = parser.acceptsAll(List.of("scope", "s"), "The scope of this enrollment token, can be either \"node\" or \"kibana\"")
            .withRequiredArg()
            .required();
    }

    public static void main(String[] args) throws Exception {
        exit(new CreateEnrollmentTokenTool().main(args, Terminal.DEFAULT));
    }

    @Override
    protected void validate(Terminal terminal, OptionSet options, Environment env) throws Exception {
        if (XPackSettings.ENROLLMENT_ENABLED.get(env.settings()) == false) {
            throw new UserException(
                ExitCodes.CONFIG,
                "[xpack.security.enrollment.enabled] must be set to `true` to create an enrollment token"
            );
        }
        final String tokenScope = scope.value(options);
        if (ALLOWED_SCOPES.contains(tokenScope) == false) {
            terminal.errorPrintln("The scope of this enrollment token, can only be one of " + ALLOWED_SCOPES);
            throw new UserException(ExitCodes.USAGE, "Invalid scope");
        }
    }

    @Override
    protected void executeCommand(Terminal terminal, OptionSet options, Environment env, String username, SecureString password)
        throws Exception {
        final String tokenScope = scope.value(options);
        final URL baseUrl = options.has(urlOption)
            ? new URL(options.valueOf(urlOption))
            : new URL(clientFunction.apply(env).getDefaultURL());
        try {
            ExternalEnrollmentTokenGenerator externalEnrollmentTokenGenerator = createEnrollmentTokenFunction.apply(env);
            if (tokenScope.equals("node")) {
                terminal.println(externalEnrollmentTokenGenerator.createNodeEnrollmentToken(username, password, baseUrl).getEncoded());
            } else {
                terminal.println(externalEnrollmentTokenGenerator.createKibanaEnrollmentToken(username, password, baseUrl).getEncoded());
            }
        } catch (Exception e) {
            terminal.errorPrintln("Unable to create enrollment token for scope [" + tokenScope + "]");
            throw new UserException(ExitCodes.CANT_CREATE, e.getMessage(), e.getCause());
        }
    }
}
