/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.file.tool;

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

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.MultiCommand;
import org.elasticsearch.cli.UserError;
import org.elasticsearch.common.Strings;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.shield.authc.Realms;
import org.elasticsearch.shield.authc.file.FileUserPasswdStore;
import org.elasticsearch.shield.authc.file.FileUserRolesStore;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authz.store.FileRolesStore;
import org.elasticsearch.shield.authz.store.ReservedRolesStore;
import org.elasticsearch.shield.support.FileAttributesChecker;
import org.elasticsearch.shield.support.Validation;
import org.elasticsearch.shield.support.Validation.Users;

public class UsersTool extends MultiCommand {

    public static void main(String[] args) throws Exception {
        Environment env = InternalSettingsPreparer.prepareEnvironment(Settings.EMPTY, Terminal.DEFAULT);
        exit(new UsersTool(env).main(args, Terminal.DEFAULT));
    }

    UsersTool(Environment env) {
        super("Manages elasticsearch native users");
        subcommands.put("useradd", new AddUserCommand(env));
        subcommands.put("userdel", new DeleteUserCommand(env));
        subcommands.put("passwd", new PasswordCommand(env));
        subcommands.put("roles", new RolesCommand(env));
        subcommands.put("list", new ListCommand(env));
    }

    static class AddUserCommand extends Command {

        private final Environment env;
        private final OptionSpec<String> passwordOption;
        private final OptionSpec<String> rolesOption;
        private final OptionSpec<String> arguments;

        AddUserCommand(Environment env) {
            super("Adds a native user");
            this.env = env;
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
            terminal.println("be added to the users file and its roles will be added to the");
            terminal.println("users_roles file. If non-default files are used (different file");
            terminal.println("locations are configured in elasticsearch.yml) the appropriate files");
            terminal.println("will be resolved from the settings and the user and its roles will be");
            terminal.println("added to them.");
            terminal.println("");
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options) throws Exception {
            String username = parseUsername(arguments.values(options));
            Validation.Error validationError = Users.validateUsername(username);
            if (validationError != null) {
                throw new UserError(ExitCodes.DATA_ERROR, "Invalid username [" + username + "]... " + validationError);
            }

            char[] password = parsePassword(terminal, passwordOption.value(options));
            String[] roles = parseRoles(terminal, env, rolesOption.value(options));

            Settings fileSettings = Realms.fileRealmSettings(env.settings());
            Path passwordFile = FileUserPasswdStore.resolveFile(fileSettings, env);
            Path rolesFile = FileUserRolesStore.resolveFile(fileSettings, env);
            FileAttributesChecker attributesChecker = new FileAttributesChecker(passwordFile, rolesFile);

            Map<String, char[]> users = new HashMap<>(FileUserPasswdStore.parseFile(passwordFile, null));
            if (users.containsKey(username)) {
                throw new UserError(ExitCodes.CODE_ERROR, "User [" + username + "] already exists");
            }
            Hasher hasher = Hasher.BCRYPT;
            users.put(username, hasher.hash(new SecuredString(password)));
            FileUserPasswdStore.writeFile(users, passwordFile);

            if (roles.length > 0) {
                Map<String, String[]> userRoles = new HashMap<>(FileUserRolesStore.parseFile(rolesFile, null));
                userRoles.put(username, roles);
                FileUserRolesStore.writeFile(userRoles, rolesFile);
            }

            attributesChecker.check(terminal);
        }
    }

    static class DeleteUserCommand extends Command {

        private final Environment env;
        private final OptionSpec<String> arguments;

        DeleteUserCommand(Environment env) {
            super("Deletes a file based user");
            this.env = env;
            this.arguments = parser.nonOptions("username");
        }

        @Override
        protected void printAdditionalHelp(Terminal terminal) {
            terminal.println("Removes an existing file based user from elasticsearch. The user will be");
            terminal.println("removed from the users file and its roles will be removed to the");
            terminal.println("users_roles file. If non-default files are used (different file");
            terminal.println("locations are configured in elasticsearch.yml) the appropriate files");
            terminal.println("will be resolved from the settings and the user and its roles will be");
            terminal.println("removed to them.");
            terminal.println("");
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options) throws Exception {
            String username = parseUsername(arguments.values(options));
            Settings fileSettings = Realms.fileRealmSettings(env.settings());
            Path passwordFile = FileUserPasswdStore.resolveFile(fileSettings, env);
            Path rolesFile = FileUserRolesStore.resolveFile(fileSettings, env);
            FileAttributesChecker attributesChecker = new FileAttributesChecker(passwordFile, rolesFile);

            Map<String, char[]> users = new HashMap<>(FileUserPasswdStore.parseFile(passwordFile, null));
            if (users.containsKey(username) == false) {
                throw new UserError(ExitCodes.NO_USER, "User [" + username + "] doesn't exist");
            }
            if (Files.exists(passwordFile)) {
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

    static class PasswordCommand extends Command {

        private final Environment env;
        private final OptionSpec<String> passwordOption;
        private final OptionSpec<String> arguments;

        PasswordCommand(Environment env) {
            super("Changes the password of an existing file based user");
            this.env = env;
            this.passwordOption = parser.acceptsAll(Arrays.asList("p", "password"),
                "The user password")
                .withRequiredArg();
            this.arguments = parser.nonOptions("username");
        }

        @Override
        protected void printAdditionalHelp(Terminal terminal) {
            terminal.println("The passwd command changes passwords for files based users. The tool");
            terminal.println("prompts twice for a replacement password. The second entry is compared");
            terminal.println("against the first and both are required to match in order for the");
            terminal.println("password to be changed. If non-default users file is used (a different");
            terminal.println("file location is configured in elasticsearch.yml) the  appropriate file");
            terminal.println("will be resolved from the settings.");
            terminal.println("");
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options) throws Exception {
            String username = parseUsername(arguments.values(options));
            char[] password = parsePassword(terminal, passwordOption.value(options));

            Settings fileSettings = Realms.fileRealmSettings(env.settings());
            Path file = FileUserPasswdStore.resolveFile(fileSettings, env);
            FileAttributesChecker attributesChecker = new FileAttributesChecker(file);
            Map<String, char[]> users = new HashMap<>(FileUserPasswdStore.parseFile(file, null));
            if (users.containsKey(username) == false) {
                throw new UserError(ExitCodes.NO_USER, "User [" + username + "] doesn't exist");
            }
            users.put(username, Hasher.BCRYPT.hash(new SecuredString(password)));
            FileUserPasswdStore.writeFile(users, file);

            attributesChecker.check(terminal);
        }
    }

    static class RolesCommand extends Command {

        private final Environment env;
        private final OptionSpec<String> addOption;
        private final OptionSpec<String> removeOption;
        private final OptionSpec<String> arguments;

        RolesCommand(Environment env) {
            super("Edit roles of an existing user");
            this.env = env;
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
        protected void execute(Terminal terminal, OptionSet options) throws Exception {
            String username = parseUsername(arguments.values(options));
            String[] addRoles = parseRoles(terminal, env, addOption.value(options));
            String[] removeRoles = parseRoles(terminal, env, removeOption.value(options));

            // check if just need to return data as no write operation happens
            // Nothing to add, just list the data for a username
            boolean readOnlyUserListing = removeRoles.length == 0 && addRoles.length == 0;
            if (readOnlyUserListing) {
                listUsersAndRoles(terminal, env, username);
                return;
            }

            Settings fileSettings = Realms.fileRealmSettings(env.settings());
            Path usersFile = FileUserPasswdStore.resolveFile(fileSettings, env);
            Path rolesFile = FileUserRolesStore.resolveFile(fileSettings, env);
            FileAttributesChecker attributesChecker = new FileAttributesChecker(usersFile, rolesFile);

            Map<String, char[]> usersMap = FileUserPasswdStore.parseFile(usersFile, null);
            if (!usersMap.containsKey(username)) {
                throw new UserError(ExitCodes.NO_USER, "User [" + username + "] doesn't exist");
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
            if (roles.size() == 0) {
                userRolesToWrite.remove(username);
            } else {
                userRolesToWrite.put(username, new LinkedHashSet<>(roles).toArray(new String[]{}));
            }
            FileUserRolesStore.writeFile(userRolesToWrite, rolesFile);

            attributesChecker.check(terminal);
        }
    }

    static class ListCommand extends Command {

        private final Environment env;
        private final OptionSpec<String> arguments;

        ListCommand(Environment env) {
            super("List existing file based users and their corresponding roles");
            this.env = env;
            this.arguments = parser.nonOptions("username");
        }

        @Override
        protected void printAdditionalHelp(Terminal terminal) {
            terminal.println("");
            terminal.println("");
        }

        @Override
        protected void execute(Terminal terminal, OptionSet options) throws Exception {
            String username = null;
            if (options.has(arguments)) {
                username = arguments.value(options);
            }
            listUsersAndRoles(terminal, env, username);
        }
    }

    // pkg private for tests
    static void listUsersAndRoles(Terminal terminal, Environment env, String username) throws Exception {
        Settings fileSettings = Realms.fileRealmSettings(env.settings());
        Path userRolesFilePath = FileUserRolesStore.resolveFile(fileSettings, env);
        Map<String, String[]> userRoles = FileUserRolesStore.parseFile(userRolesFilePath, null);

        Path userFilePath = FileUserPasswdStore.resolveFile(fileSettings, env);
        Set<String> users = FileUserPasswdStore.parseFile(userFilePath, null).keySet();

        Path rolesFilePath = FileRolesStore.resolveFile(env.settings(), env);
        Set<String> knownRoles = Sets.union(FileRolesStore.parseFileForRoleNames(rolesFilePath, null), ReservedRolesStore.names());

        if (username != null) {
            if (!users.contains(username)) {
                throw new UserError(ExitCodes.NO_USER, "User [" + username + "] doesn't exist");
            }

            if (userRoles.containsKey(username)) {
                String[] roles = userRoles.get(username);
                Set<String> unknownRoles = Sets.difference(Sets.newHashSet(roles), knownRoles);
                String[] markedRoles = markUnknownRoles(roles, unknownRoles);
                terminal.println(String.format(Locale.ROOT, "%-15s: %s", username, Arrays.stream(markedRoles).map(s -> s == null ?
                    "-" : s).collect(Collectors.joining(","))));
                if (!unknownRoles.isEmpty()) {
                    // at least one role is marked... so printing the legend
                    Path rolesFile = FileRolesStore.resolveFile(fileSettings, env).toAbsolutePath();
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
            Set<String> usersWithoutRoles = Sets.newHashSet(users);
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
                Path rolesFile = FileRolesStore.resolveFile(fileSettings, env).toAbsolutePath();
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
    static String parseUsername(List<String> args) throws UserError {
        if (args.isEmpty()) {
            throw new UserError(ExitCodes.USAGE, "Missing username argument");
        } else if (args.size() > 1) {
            throw new UserError(ExitCodes.USAGE, "Expected a single username argument, found extra: " + args.toString());
        }
        String username = args.get(0);
        Validation.Error validationError = Users.validateUsername(username);
        if (validationError != null) {
            throw new UserError(ExitCodes.DATA_ERROR, "Invalid username [" + username + "]... " + validationError);
        }
        return username;
    }

    // pkg private for testing
    static char[] parsePassword(Terminal terminal, String passwordStr) throws UserError {
        char[] password;
        if (passwordStr != null) {
            password = passwordStr.toCharArray();
            Validation.Error validationError = Users.validatePassword(password);
            if (validationError != null) {
                throw new UserError(ExitCodes.DATA_ERROR, "Invalid password..." + validationError);
            }
        } else {
            password = terminal.readSecret("Enter new password: ");
            Validation.Error validationError = Users.validatePassword(password);
            if (validationError != null) {
                throw new UserError(ExitCodes.DATA_ERROR, "Invalid password..." + validationError);
            }
            char[] retyped = terminal.readSecret("Retype new password: ");
            if (Arrays.equals(password, retyped) == false) {
                throw new UserError(ExitCodes.DATA_ERROR, "Password mismatch");
            }
        }
        return password;
    }

    private static void verifyRoles(Terminal terminal, Settings settings, Environment env, String[] roles) {
        Path rolesFile = FileRolesStore.resolveFile(settings, env);
        assert Files.exists(rolesFile);
        Set<String> knownRoles = Sets.union(FileRolesStore.parseFileForRoleNames(rolesFile, null), ReservedRolesStore.names());
        Set<String> unknownRoles = Sets.difference(Sets.newHashSet(roles), knownRoles);
        if (!unknownRoles.isEmpty()) {
            terminal.println(String.format(Locale.ROOT, "Warning: The following roles [%s] are not in the [%s] file. Make sure the names " +
                    "are correct. If the names are correct and the roles were created using the API please disregard this message. " +
                    "Nonetheless the user will still be associated with all specified roles",
                Strings.collectionToCommaDelimitedString(unknownRoles), rolesFile.toAbsolutePath()));
            terminal.println("Known roles: " + knownRoles.toString());
        }
    }

    // pkg private for testing
    static String[] parseRoles(Terminal terminal, Environment env, String rolesStr) throws UserError {
        if (rolesStr.isEmpty()) {
            return Strings.EMPTY_ARRAY;
        }
        String[] roles = rolesStr.split(",");
        for (String role : roles) {
            Validation.Error validationError = Validation.Roles.validateRoleName(role);
            if (validationError != null) {
                throw new UserError(ExitCodes.DATA_ERROR, "Invalid role [" + role + "]... " + validationError);
            }
        }

        Settings fileSettings = Realms.fileRealmSettings(env.settings());
        verifyRoles(terminal, fileSettings, env, roles);

        return roles;
    }
}
