/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.common.cli.LoggingAwareMultiCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.security.authc.service.ServiceAccount.ServiceAccountId;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountToken.ServiceAccountTokenId;
import org.elasticsearch.xpack.security.support.FileAttributesChecker;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;

public class FileTokensTool extends LoggingAwareMultiCommand {

    public static void main(String[] args) throws Exception {
        exit(new FileTokensTool().main(args, Terminal.DEFAULT));
    }

    public FileTokensTool() {
        super("Manages elasticsearch service account file-tokens");
        subcommands.put("create", newCreateFileTokenCommand());
        subcommands.put("delete", newDeleteFileTokenCommand());
        subcommands.put("list", newListFileTokenCommand());
    }

    protected CreateFileTokenCommand newCreateFileTokenCommand() {
        return new CreateFileTokenCommand();
    }

    protected DeleteFileTokenCommand newDeleteFileTokenCommand() {
        return new DeleteFileTokenCommand();
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
            final ServiceAccountTokenId accountTokenId = parsePrincipalAndTokenName(arguments.values(options), env.settings());
            final Hasher hasher = Hasher.resolve(XPackSettings.SERVICE_TOKEN_HASHING_ALGORITHM.get(env.settings()));
            final Path serviceTokensFile = FileServiceAccountTokenStore.resolveFile(env);

            FileAttributesChecker attributesChecker = new FileAttributesChecker(serviceTokensFile);
            final Map<String, char[]> tokenHashes = new TreeMap<>(FileServiceAccountTokenStore.parseFile(serviceTokensFile, null));

            try (ServiceAccountToken token = ServiceAccountToken.newToken(accountTokenId.getAccountId(), accountTokenId.getTokenName())) {
                if (tokenHashes.containsKey(token.getQualifiedName())) {
                    throw new UserException(ExitCodes.CODE_ERROR, "Service token [" + token.getQualifiedName() + "] already exists");
                }
                tokenHashes.put(token.getQualifiedName(), hasher.hash(token.getSecret()));
                FileServiceAccountTokenStore.writeFile(serviceTokensFile, tokenHashes);
                terminal.println("SERVICE_TOKEN " + token.getQualifiedName() + " = " + token.asBearerString());
            }

            attributesChecker.check(terminal);
        }

    }

    static class DeleteFileTokenCommand extends EnvironmentAwareCommand {

        private final OptionSpec<String> arguments;

        DeleteFileTokenCommand() {
            super("Remove a file token for specified service account and token name");
            this.arguments = parser.nonOptions("service-account-principal token-name");
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
            final ServiceAccountTokenId accountTokenId = parsePrincipalAndTokenName(arguments.values(options), env.settings());
            final String qualifiedName = accountTokenId.getQualifiedName();
            final Path serviceTokensFile = FileServiceAccountTokenStore.resolveFile(env);

            FileAttributesChecker attributesChecker = new FileAttributesChecker(serviceTokensFile);
            final Map<String, char[]> tokenHashes = new TreeMap<>(FileServiceAccountTokenStore.parseFile(serviceTokensFile, null));

            if (tokenHashes.remove(qualifiedName) == null) {
                throw new UserException(ExitCodes.CODE_ERROR, "Service token [" + qualifiedName + "] does not exist");
            } else {
                FileServiceAccountTokenStore.writeFile(serviceTokensFile, tokenHashes);
            }
            attributesChecker.check(terminal);
        }
    }

    static class ListFileTokenCommand extends EnvironmentAwareCommand {

        private final OptionSpec<String> arguments;

        ListFileTokenCommand() {
            super("List file tokens for the specified service account");
            this.arguments = parser.nonOptions("service-account-principal");
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
            final List<String> args = arguments.values(options);
            if (args.size() > 1) {
                throw new UserException(
                    ExitCodes.USAGE,
                    "Expected at most one argument, service-account-principal, found extra: ["
                        + Strings.collectionToCommaDelimitedString(args)
                        + "]"
                );
            }
            Predicate<String> filter = k -> true;
            if (args.size() == 1) {
                final String principal = args.get(0);
                if (false == ServiceAccountService.isServiceAccountPrincipal(principal)) {
                    throw new UserException(
                        ExitCodes.NO_USER,
                        "Unknown service account principal: ["
                            + principal
                            + "]. Must be one of ["
                            + Strings.collectionToDelimitedString(ServiceAccountService.getServiceAccountPrincipals(), ",")
                            + "]"
                    );
                }
                filter = filter.and(k -> k.startsWith(principal + "/"));
            }
            final Path serviceTokensFile = FileServiceAccountTokenStore.resolveFile(env);
            final Map<String, char[]> tokenHashes = new TreeMap<>(FileServiceAccountTokenStore.parseFile(serviceTokensFile, null));
            for (String key : tokenHashes.keySet()) {
                if (filter.test(key)) {
                    terminal.println(key);
                }
            }
        }
    }

    static ServiceAccountTokenId parsePrincipalAndTokenName(List<String> arguments, Settings settings) throws UserException {
        if (arguments.isEmpty()) {
            throw new UserException(ExitCodes.USAGE, "Missing service-account-principal and token-name arguments");
        } else if (arguments.size() == 1) {
            throw new UserException(ExitCodes.USAGE, "Missing token-name argument");
        } else if (arguments.size() > 2) {
            throw new UserException(
                ExitCodes.USAGE,
                "Expected two arguments, service-account-principal and token-name, found extra: ["
                    + Strings.collectionToCommaDelimitedString(arguments)
                    + "]"
            );
        }
        final String principal = arguments.get(0);
        final String tokenName = arguments.get(1);
        if (false == ServiceAccountService.isServiceAccountPrincipal(principal)) {
            throw new UserException(
                ExitCodes.NO_USER,
                "Unknown service account principal: ["
                    + principal
                    + "]. Must be one of ["
                    + Strings.collectionToDelimitedString(ServiceAccountService.getServiceAccountPrincipals(), ",")
                    + "]"
            );
        }
        if (false == Validation.isValidServiceAccountTokenName(tokenName)) {
            throw new UserException(ExitCodes.CODE_ERROR, Validation.formatInvalidServiceTokenNameErrorMessage(tokenName));
        }
        return new ServiceAccountTokenId(ServiceAccountId.fromPrincipal(principal), tokenName);
    }
}
