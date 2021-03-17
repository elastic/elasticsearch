/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.LoggingAwareMultiCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.security.support.FileAttributesChecker;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileTokensTool extends LoggingAwareMultiCommand {

    public static void main(String[] args) throws Exception {
        exit(new FileTokensTool().main(args, Terminal.DEFAULT));
    }

    public FileTokensTool() {
        super("Manages elasticsearch service account file-tokens");
        subcommands.put("create", newCreateFileTokenCommand());
        subcommands.put("remove", newRemoveFileTokenCommand());
        subcommands.put("list", newListFileTokenCommand());
    }

    protected CreateFileTokenCommand newCreateFileTokenCommand() {
        return new CreateFileTokenCommand();
    }

    protected RemoveFileTokenCommand newRemoveFileTokenCommand() {
        return new RemoveFileTokenCommand();
    }

    protected ListFileTokenCommand newListFileTokenCommand() {
        return new ListFileTokenCommand();
    }

    static class CreateFileTokenCommand extends EnvironmentAwareCommand {

        private final OptionSpec<String> arguments;

        CreateFileTokenCommand() {
            super("Create a file token for specified service account and token name");
            this.arguments = parser.nonOptions("service-account-principal token-name");
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
            final Tuple<String, String> tuple = parsePrincipalAndTokenName(arguments.values(options), env.settings());
            final String principal = tuple.v1();
            final String tokenName = tuple.v2();
            if (false == ServiceAccountService.isServiceAccountPrincipal(principal)) {
                throw new UserException(ExitCodes.NO_USER, "Unknown service account principal: [" + principal + "]. Must be one of ["
                    + Strings.collectionToDelimitedString(ServiceAccountService.getServiceAccountPrincipals(), ",") + "]");
            }
            final Hasher hasher = Hasher.resolve(XPackSettings.SERVICE_TOKEN_HASHING_ALGORITHM.get(env.settings()));
            final Path serviceTokensFile = FileServiceAccountsTokenStore.resolveFile(env);

            FileAttributesChecker attributesChecker = new FileAttributesChecker(serviceTokensFile);
            final Map<String, char[]> tokenHashes = new HashMap<>(FileServiceAccountsTokenStore.parseFile(serviceTokensFile, null));

            try (SecureString tokenString = UUIDs.randomBase64UUIDSecureString()) {
                final ServiceAccountToken token =
                    new ServiceAccountToken(ServiceAccountId.fromPrincipal(principal), tokenName, tokenString);
                if (tokenHashes.containsKey(token.getQualifiedName())) {
                    throw new UserException(ExitCodes.CODE_ERROR, "Service token [" + token.getQualifiedName() + "] already exists");
                }
                tokenHashes.put(token.getQualifiedName(), hasher.hash(token.getSecret()));
                FileServiceAccountsTokenStore.writeFile(serviceTokensFile, tokenHashes);
                terminal.println("SERVICE_TOKEN " + token.getQualifiedName() + " = " + token.asBearerString());
            }

            attributesChecker.check(terminal);
        }

        static Tuple<String, String> parsePrincipalAndTokenName(List<String> arguments, Settings settings) throws UserException {
            if (arguments.isEmpty()) {
                throw new UserException(ExitCodes.USAGE, "Missing service-account-principal and token-name arguments");
            } else if (arguments.size() == 1) {
                throw new UserException(ExitCodes.USAGE, "Missing token-name argument");
            } else if (arguments.size() > 2) {
                throw new UserException(
                    ExitCodes.USAGE,
                    "Expected two arguments, service-account-principal and token-name, found extra: " + arguments.toString());
            }
            return new Tuple<>(arguments.get(0), arguments.get(1));
        }
    }

    static class RemoveFileTokenCommand extends EnvironmentAwareCommand {

        RemoveFileTokenCommand() {
            super("Remove a file token for specified service account and token name");
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
            throw new UnsupportedOperationException("remove command not implemented yet");
        }
    }

    static class ListFileTokenCommand extends EnvironmentAwareCommand {

        ListFileTokenCommand() {
            super("List file tokens for the specified service account");
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
            throw new UnsupportedOperationException("list command not implemented yet");
        }
    }
}
