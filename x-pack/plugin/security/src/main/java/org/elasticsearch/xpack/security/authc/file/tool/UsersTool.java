/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.file.tool;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.LoggingAwareMultiCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.core.security.support.Validation.Users;
import org.elasticsearch.xpack.security.authc.file.FileUserPasswdStore;
import org.elasticsearch.xpack.security.authc.file.FileUserRolesStore;
import org.elasticsearch.xpack.security.authz.store.FileRolesStore;
import org.elasticsearch.xpack.security.support.FileAttributesChecker;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class UsersTool extends LoggingAwareMultiCommand {

    public static void main(String[] args) throws Exception {
        exit(new UsersTool().main(args, Terminal.DEFAULT));
    }

    UsersTool() {
        super("Manages elasticsearch file users");
        subcommands.put("useradd", newAddUserCommand());
        subcommands.put("userdel", newDeleteUserCommand());
        subcommands.put("passwd", newPasswordCommand());
        subcommands.put("roles", newRolesCommand());
        subcommands.put("list", newListCommand());
    }

    protected AddUserCommand newAddUserCommand() {
        return new AddUserCommand();
    }

    protected DeleteUserCommand newDeleteUserCommand() {
        return new DeleteUserCommand();
    }

    protected PasswordCommand newPasswordCommand() {
        return new PasswordCommand();
    }

    protected RolesCommand newRolesCommand() {
        return new RolesCommand();
    }

    protected ListCommand newListCommand() {
        return new ListCommand();
    }

    static class AddUserCommand extends EnvironmentAwareCommand {

        private final OptionSpec<String> passwordOption;
        private final OptionSpec<String> rolesOption;
        private final OptionSpec<String> arguments;

        AddUserCommand() {
            super("Adds a file user");

            this.passwordOption = parser.acceptsAll(Arrays.asList("p", "password"),
                "The user password")
                .withRequiredArg();
            this.rolesOption = parser.acceptsAll(Arrays.asList("r", "roles"),
                "Comma-separated list of the roles of the user")
                .withRequiredArg().defaultsTo("");
            this.arguments = parser.nonOptions("username");
        }

        @Override
        protected void printAdditionalHelp(Terminal terminal) {
            terminal.println("Adds a file based user to elasticsearch (via internal realm). The user will");
            terminal.println("be added to the \"users\" file and its roles will be added to the");
            terminal.println("\"users_roles\" file in the elasticsearch config directory.");
            terminal.println("");
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {

            String username = parseUsername(arguments.values(options), env.settings());
            final boolean allowReserved = XPackSettings.RESERVED_REALM_ENABLED_SETTING.get(env.settings()) == false;
            Validation.Error validationError = Users.validateUsername(username, allowReserved, env.settings());
            if (validationError != null) {
                throw new UserException(ExitCodes.DATA_ERROR, "Invalid username [" + username + "]... " + validationError);
            }

            final char[] passwordHash = getPasswordHash(terminal, env, passwordOption.value(options));
            String[] roles = parseRoles(terminal, env, rolesOption.value(options));

            Path passwordFile = FileUserPasswdStore.resolveFile(env);
            Path rolesFile = FileUserRolesStore.resolveFile(env);
            FileAttributesChecker attributesChecker = new FileAttributesChecker(passwordFile, rolesFile);

            Map<String, char[]> users = FileUserPasswdStore.parseFile(passwordFile, null, env.settings());
            if (users == null) {
                throw new UserException(ExitCodes.CONFIG, "Configuration file [" + passwordFile + "] is missing");
            }
            if (users.containsKey(username)) {
                throw new UserException(ExitCodes.CODE_ERROR, "User [" + username + "] already exists");
            }
            users = new HashMap<>(users); // make modifiable
            users.put(username, passwordHash);
            FileUserPasswdStore.writeFile(users, passwordFile);

            if (roles.length > 0) {
                Map<String, String[]> userRoles = new HashMap<>(FileUserRolesStore.parseFile(rolesFile, null));
                userRoles.put(username, roles);
                FileUserRolesStore.writeFile(userRoles, rolesFile);
            }

            attributesChecker.check(terminal);
        }
    }

    static class DeleteUserCommand extends EnvironmentAwareCommand {

        private final OptionSpec<String> arguments;

        DeleteUserCommand() {
            super("Deletes a file based user");
            this.arguments = parser.nonOptions("username");
        }

        @Override
        protected void printAdditionalHelp(Terminal terminal) {
            terminal.println("Removes an existing file based user from elasticsearch. The user will be");
            terminal.println("removed from the \"users\" file and its roles will be removed from the");
            terminal.println("\"users_roles\" file in the elasticsearch config directory.");
            terminal.println("");
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {

            String username = parseUsername(arguments.values(options), env.settings());
            Path passwordFile = FileUserPasswdStore.resolveFile(env);
            Path rolesFile = FileUserRolesStore.resolveFile(env);
            FileAttributesChecker attributesChecker = new FileAttributesChecker(passwordFile, rolesFile);

            Map<String, char[]> users = FileUserPasswdStore.parseFile(passwordFile, null, env.settings());
            if (users == null) {
                throw new UserException(ExitCodes.CONFIG, "Configuration file [" + passwordFile + "] is missing");
            }
            if (users.containsKey(username) == false) {
                throw new UserException(ExitCodes.NO_USER, "User [" + username + "] doesn't exist");
            }
            if (Files.exists(passwordFile)) {
                users = new HashMap<>(users);
                char[] passwd = users.remove(username);
                if (passwd != null) {
                    FileUserPasswdStore.writeFile(users, passwordFile);
                }
            }

            Map<String, String[]> userRoles = new HashMap<>(FileUserRolesStore.parseFile(rolesFile, null));
            if (Files.exists(rolesFile)) {
                String[] roles = userRoles.remove(username);
                if (roles != null) {
                    FileUserRolesStore.writeFile(userRoles, rolesFile);
                }
            }

            attributesChecker.check(terminal);
        }
    }

    static class PasswordCommand extends EnvironmentAwareCommand {

        private final OptionSpec<String> passwordOption;
        private final OptionSpec<String> arguments;

        PasswordCommand() {
            super("Changes the password of an existing file based user");
            this.passwordOption = parser.acceptsAll(Arrays.asList("p", "password"),
                "The user password")
                .withRequiredArg();
            this.arguments = parser.nonOptions("username");
        }

        @Override
        protected void printAdditionalHelp(Terminal terminal) {
            terminal.println("The passwd command changes passwords for file based users. The tool");
            terminal.println("prompts twice for a replacement password. The second entry is compared");
            terminal.println("against the first and both are required to match in order for the");
            terminal.println("password to be changed.");
            terminal.println("");
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {

            String username = parseUsername(arguments.values(options), env.settings());
            char[] passwordHash = getPasswordHash(terminal, env, passwordOption.value(options));

            Path file = FileUserPasswdStore.resolveFile(env);
            FileAttributesChecker attributesChecker = new FileAttributesChecker(file);
            Map<String, char[]> users = FileUserPasswdStore.parseFile(file, null, env.settings());
            if (users == null) {
                throw new UserException(ExitCodes.CONFIG, "Configuration file [" + file + "] is missing");
            }
            if (users.containsKey(username) == false) {
                throw new UserException(ExitCodes.NO_USER, "User [" + username + "] doesn't exist");
            }
            users = new HashMap<>(users); // make modifiable
            users.put(username, passwordHash);
            FileUserPasswdStore.writeFile(users, file);

            attributesChecker.check(terminal);
        }
    }

    static class RolesCommand extends EnvironmentAwareCommand {

        private final OptionSpec<String> addOption;
        private final OptionSpec<String> removeOption;
        private final OptionSpec<String> arguments;

        RolesCommand() {
            super("Edit roles of an existing user");
            this.addOption = parser.acceptsAll(Arrays.asList("a", "add"),
                "Adds supplied roles to the specified user")
                .withRequiredArg().defaultsTo("");
            this.removeOption = parser.acceptsAll(Arrays.asList("r", "remove"),
                "Remove supplied roles from the specified user")
                .withRequiredArg().defaultsTo("");
            this.arguments = parser.nonOptions("username");
        }

        @Override
        protected void printAdditionalHelp(Terminal terminal) {
            terminal.println("The roles command allows editing roles for file based users.");
            terminal.println("You can also list a user's roles by omitting the -a and -r");
            terminal.println("parameters.");
            terminal.println("");
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {

            String username = parseUsername(arguments.values(options), env.settings());
            String[] addRoles = parseRoles(terminal, env, addOption.value(options));
            String[] removeRoles = parseRoles(terminal, env, removeOption.value(options));

            // check if just need to return data as no write operation happens
            // Nothing to add, just list the data for a username
            boolean readOnlyUserListing = removeRoles.length == 0 && addRoles.length == 0;
            if (readOnlyUserListing) {
                listUsersAndRoles(terminal, env, username);
                return;
            }

            Path usersFile = FileUserPasswdStore.resolveFile(env);
            Path rolesFile = FileUserRolesStore.resolveFile(env);
            FileAttributesChecker attributesChecker = new FileAttributesChecker(usersFile, rolesFile);

            Map<String, char[]> usersMap = FileUserPasswdStore.parseFile(usersFile, null, env.settings());
            if (!usersMap.containsKey(username)) {
                throw new UserException(ExitCodes.NO_USER, "User [" + username + "] doesn't exist");
            }

            Map<String, String[]> userRoles = FileUserRolesStore.parseFile(rolesFile, null);
            List<String> roles = new ArrayList<>();
            if (userRoles.get(username) != null) {
                roles.addAll(Arrays.asList(userRoles.get(username)));
            }
            roles.addAll(Arrays.asList(addRoles));
            roles.removeAll(Arrays.asList(removeRoles));

            Map<String, String[]> userRolesToWrite = new HashMap<>(userRoles.size());
            userRolesToWrite.putAll(userRoles);
            if (roles.isEmpty()) {
                userRolesToWrite.remove(username);
            } else {
                userRolesToWrite.put(username, new LinkedHashSet<>(roles).toArray(new String[]{}));
            }
            FileUserRolesStore.writeFile(userRolesToWrite, rolesFile);

            attributesChecker.check(terminal);
        }
    }

    static class ListCommand extends EnvironmentAwareCommand {

        private final OptionSpec<String> arguments;

        ListCommand() {
            super("List existing file based users and their corresponding roles");
            this.arguments = parser.nonOptions("username");
        }

        @Override
        protected void printAdditionalHelp(Terminal terminal) {
            terminal.println("");
            terminal.println("");
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {

            String username = null;
            if (options.has(arguments)) {
                username = arguments.value(options);
            }
            listUsersAndRoles(terminal, env, username);
        }
    }

    // pkg private for tests
    static void listUsersAndRoles(Terminal terminal, Environment env, String username) throws Exception {
        Path userRolesFilePath = FileUserRolesStore.resolveFile(env);
        Map<String, String[]> userRoles = FileUserRolesStore.parseFile(userRolesFilePath, null);
        if (userRoles == null) {
            throw new UserException(ExitCodes.CONFIG, "Configuration file [" + userRolesFilePath + "] is missing");
        }

        Path userFilePath = FileUserPasswdStore.resolveFile(env);
        Map<String, char[]> users = FileUserPasswdStore.parseFile(userFilePath, null, env.settings());
        if (users == null) {
            throw new UserException(ExitCodes.CONFIG, "Configuration file [" + userFilePath + "] is missing");
        }

        Path rolesFilePath = FileRolesStore.resolveFile(env);
        Set<String> knownRoles = Sets.union(FileRolesStore.parseFileForRoleNames(rolesFilePath, null), ReservedRolesStore.names());
        if (knownRoles == null) {
            throw new UserException(ExitCodes.CONFIG, "Configuration file [" + rolesFilePath + "] is missing");
        }

        if (username != null) {
            if (!users.containsKey(username)) {
                throw new UserException(ExitCodes.NO_USER, "User [" + username + "] doesn't exist");
            }

            if (userRoles.containsKey(username)) {
                String[] roles = userRoles.get(username);
                Set<String> unknownRoles = Sets.difference(Sets.newHashSet(roles), knownRoles);
                String[] markedRoles = markUnknownRoles(roles, unknownRoles);
                terminal.println(String.format(Locale.ROOT, "%-15s: %s", username, Arrays.stream(markedRoles).map(s -> s == null ?
                    "-" : s).collect(Collectors.joining(","))));
                if (!unknownRoles.isEmpty()) {
                    // at least one role is marked... so printing the legend
                    Path rolesFile = FileRolesStore.resolveFile(env).toAbsolutePath();
                    terminal.println("");
                    terminal.println(" [*]   Role is not in the [" + rolesFile.toAbsolutePath() + "] file. If the role has been created "
                        + "using the API, please disregard this message.");
                }
            } else {
                terminal.println(String.format(Locale.ROOT, "%-15s: -", username));
            }
        } else {
            boolean unknownRolesFound = false;
            boolean usersExist = false;
            for (Map.Entry<String, String[]> entry : userRoles.entrySet()) {
                String[] roles = entry.getValue();
                Set<String> unknownRoles = Sets.difference(Sets.newHashSet(roles), knownRoles);
                String[] markedRoles = markUnknownRoles(roles, unknownRoles);
                terminal.println(String.format(Locale.ROOT, "%-15s: %s", entry.getKey(), String.join(",", markedRoles)));
                unknownRolesFound = unknownRolesFound || !unknownRoles.isEmpty();
                usersExist = true;
            }
            // list users without roles
            Set<String> usersWithoutRoles = Sets.newHashSet(users.keySet());
            usersWithoutRoles.removeAll(userRoles.keySet());
            for (String user : usersWithoutRoles) {
                terminal.println(String.format(Locale.ROOT, "%-15s: -", user));
                usersExist = true;
            }

            if (!usersExist) {
                terminal.println("No users found");
                return;
            }

            if (unknownRolesFound) {
                // at least one role is marked... so printing the legend
                Path rolesFile = FileRolesStore.resolveFile(env).toAbsolutePath();
                terminal.println("");
                terminal.println(" [*]   Role is not in the [" + rolesFile.toAbsolutePath() + "] file. If the role has been created "
                    + "using the API, please disregard this message.");
            }
        }
    }

    private static String[] markUnknownRoles(String[] roles, Set<String> unknownRoles) {
        if (unknownRoles.isEmpty()) {
            return roles;
        }
        String[] marked = new String[roles.length];
        for (int i = 0; i < roles.length; i++) {
            if (unknownRoles.contains(roles[i])) {
                marked[i] = roles[i] + "*";
            } else {
                marked[i] = roles[i];
            }
        }
        return marked;
    }

    // pkg private for testing
    static String parseUsername(List<String> args, Settings settings) throws UserException {
        if (args.isEmpty()) {
            throw new UserException(ExitCodes.USAGE, "Missing username argument");
        } else if (args.size() > 1) {
            throw new UserException(ExitCodes.USAGE, "Expected a single username argument, found extra: " + args.toString());
        }
        String username = args.get(0);
        final boolean allowReserved = XPackSettings.RESERVED_REALM_ENABLED_SETTING.get(settings) == false;
        Validation.Error validationError = Users.validateUsername(username, allowReserved, settings);
        if (validationError != null) {
            throw new UserException(ExitCodes.DATA_ERROR, "Invalid username [" + username + "]... " + validationError);
        }
        return username;
    }

    private static char[] getPasswordHash(Terminal terminal, Environment env, String cliPasswordValue) throws UserException {
        final Hasher hasher = Hasher.resolve(XPackSettings.PASSWORD_HASHING_ALGORITHM.get(env.settings()));
        if (XPackSettings.FIPS_MODE_ENABLED.get(env.settings()) && hasher.name().toLowerCase(Locale.ROOT).startsWith("pbkdf2") == false) {
            throw new UserException(ExitCodes.CONFIG, "Only PBKDF2 is allowed for password hashing in a FIPS 140 JVM. Please set the " +
                "appropriate value for [ " + XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey() + " ] setting.");
        }
        final char[] passwordHash;
        try (SecureString password = parsePassword(terminal, cliPasswordValue)) {
            passwordHash = hasher.hash(password);
        }
        return passwordHash;
    }

    // pkg private for testing
    static SecureString parsePassword(Terminal terminal, String passwordStr) throws UserException {
        SecureString password;
        if (passwordStr != null) {
            password = new SecureString(passwordStr.toCharArray());
            Validation.Error validationError = Users.validatePassword(password);
            if (validationError != null) {
                throw new UserException(ExitCodes.DATA_ERROR, "Invalid password..." + validationError);
            }
        } else {
            password = new SecureString(terminal.readSecret("Enter new password: "));
            Validation.Error validationError = Users.validatePassword(password);
            if (validationError != null) {
                throw new UserException(ExitCodes.DATA_ERROR, "Invalid password..." + validationError);
            }
            char[] retyped = terminal.readSecret("Retype new password: ");
            if (Arrays.equals(password.getChars(), retyped) == false) {
                throw new UserException(ExitCodes.DATA_ERROR, "Password mismatch");
            }
        }
        return password;
    }

    private static void verifyRoles(Terminal terminal, Environment env, String[] roles) {
        Path rolesFile = FileRolesStore.resolveFile(env);
        assert Files.exists(rolesFile);
        Set<String> knownRoles = Sets.union(FileRolesStore.parseFileForRoleNames(rolesFile, null), ReservedRolesStore.names());
        Set<String> unknownRoles = Sets.difference(Sets.newHashSet(roles), knownRoles);
        if (!unknownRoles.isEmpty()) {
            terminal.errorPrintln(String.format(Locale.ROOT, "Warning: The following roles [%s] are not in the [%s] file. " +
                    "Make sure the names are correct. If the names are correct and the roles were created using the API please " +
                    "disregard this message. Nonetheless the user will still be associated with all specified roles",
                Strings.collectionToCommaDelimitedString(unknownRoles), rolesFile.toAbsolutePath()));
            terminal.errorPrintln("Known roles: " + knownRoles.toString());
        }
    }

    // pkg private for testing
    static String[] parseRoles(Terminal terminal, Environment env, String rolesStr) throws UserException {
        if (rolesStr.isEmpty()) {
            return Strings.EMPTY_ARRAY;
        }
        String[] roles = rolesStr.split(",");
        for (String role : roles) {
            Validation.Error validationError = Validation.Roles.validateRoleName(role, true);
            if (validationError != null) {
                throw new UserException(ExitCodes.DATA_ERROR, "Invalid role [" + role + "]... " + validationError);
            }
        }

        verifyRoles(terminal, env, roles);

        return roles;
    }
}
