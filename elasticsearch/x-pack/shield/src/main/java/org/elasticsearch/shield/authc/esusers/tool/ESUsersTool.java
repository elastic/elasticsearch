/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esusers.tool;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.cli.CommandLine;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.MultiCommand;
import org.elasticsearch.cli.UserError;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolConfig;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.shield.authc.Realms;
import org.elasticsearch.shield.authc.esusers.ESUsersRealm;
import org.elasticsearch.shield.authc.esusers.FileUserPasswdStore;
import org.elasticsearch.shield.authc.esusers.FileUserRolesStore;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authz.store.FileRolesStore;
import org.elasticsearch.shield.support.FileAttributesChecker;
import org.elasticsearch.shield.support.Validation;

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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.common.cli.CliToolConfig.Builder.cmd;

public class ESUsersTool extends MultiCommand {

    public static void main(String[] args) throws Exception {
        Environment env = InternalSettingsPreparer.prepareEnvironment(Settings.EMPTY, Terminal.DEFAULT);
        exit(new ESUsersTool(env).main(args, Terminal.DEFAULT));
    }

    ESUsersTool(Environment env) {
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
            terminal.println("Adds a native user to elasticsearch (via internal realm). The user will");
            terminal.println("be added to the users file and its roles will be added to the");
            terminal.println("users_roles file. If non-default files are used (different file");
            terminal.println("locations are configured in elasticsearch.yml) the appropriate files");
            terminal.println("will be resolved from the settings and the user and its roles will be");
            terminal.println("added to them.");
            terminal.println("");
        }

        @Override
        protected int execute(Terminal terminal, OptionSet options) throws Exception {
            String username = arguments.value(options);
            String password = passwordOption.value(options);
            String roles = rolesOption.value(options);
            execute(terminal, username, password, roles);
            return ExitCodes.OK;
        }

        // pkg private for testing
        void execute(Terminal terminal, String username, String passwordStr, String rolesCsv) throws Exception {
            Validation.Error validationError = Validation.ESUsers.validateUsername(username);
            if (validationError != null) {
                throw new UserError(ExitCodes.DATA_ERROR, "Invalid username [" + username + "]... " + validationError);
            }

            char[] password;
            if (passwordStr != null) {
                password = passwordStr.toCharArray();
                validationError = Validation.ESUsers.validatePassword(password);
                if (validationError != null) {
                    throw new UserError(ExitCodes.DATA_ERROR, "Invalid password..." + validationError);
                }
            } else {
                password = terminal.readSecret("Enter new password: ");
                validationError = Validation.ESUsers.validatePassword(password);
                if (validationError != null) {
                    throw new UserError(ExitCodes.DATA_ERROR, "Invalid password..." + validationError);
                }
                char[] retyped = terminal.readSecret("Retype new password: ");
                if (Arrays.equals(password, retyped) == false) {
                    throw new UserError(ExitCodes.DATA_ERROR, "Password mismatch");
                }
            }

            String[] roles = rolesCsv.split(",");
            for (String role : roles) {
                validationError = Validation.Roles.validateRoleName(role);
                if (validationError != null) {
                    throw new UserError(ExitCodes.DATA_ERROR, "Invalid role [" + role + "]... " + validationError);
                }
            }

            Settings esusersSettings = Realms.internalRealmSettings(env.settings(), ESUsersRealm.TYPE);
            Path passwordFile = FileUserPasswdStore.resolveFile(esusersSettings, env);
            Path rolesFile = FileUserRolesStore.resolveFile(esusersSettings, env);
            verifyRoles(terminal, env.settings(), env, roles);
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
            super("Deletes a native user");
            this.env = env;
            this.arguments = parser.nonOptions("username");
        }

        @Override
        protected void printAdditionalHelp(Terminal terminal) {
            terminal.println("Removes an existing native user from elasticsearch. The user will be");
            terminal.println("removed from the users file and its roles will be removed to the");
            terminal.println("users_roles file. If non-default files are used (different file");
            terminal.println("locations are configured in elasticsearch.yml) the appropriate files");
            terminal.println("will be resolved from the settings and the user and its roles will be");
            terminal.println("removed to them.");
            terminal.println("");
        }

        @Override
        protected int execute(Terminal terminal, OptionSet options) throws Exception {
            String username = arguments.value(options);
            execute(terminal, username);
            return ExitCodes.OK;
        }

        // pkg private for testing
        void execute(Terminal terminal, String username) throws Exception {
            Settings esusersSettings = Realms.internalRealmSettings(env.settings(), ESUsersRealm.TYPE);
            Path passwordFile = FileUserPasswdStore.resolveFile(esusersSettings, env);
            Path rolesFile = FileUserRolesStore.resolveFile(esusersSettings, env);
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
            super("Changes the password of an existing native user");
            this.env = env;
            this.passwordOption = parser.acceptsAll(Arrays.asList("p", "password"),
                "The user password")
                .withRequiredArg();
            this.arguments = parser.nonOptions("username");
        }

        @Override
        protected void printAdditionalHelp(Terminal terminal) {
            terminal.println("The passwd command changes passwords for native user accounts. The tool");
            terminal.println("prompts twice for a replacement password. The second entry is compared");
            terminal.println("against the first and both are required to match in order for the");
            terminal.println("password to be changed. If non-default users file is used (a different");
            terminal.println("file location is configured in elasticsearch.yml) the  appropriate file");
            terminal.println("will be resolved from the settings.");
            terminal.println("");
        }

        @Override
        protected int execute(Terminal terminal, OptionSet options) throws Exception {
            String username = arguments.value(options);
            String password = passwordOption.value(options);
            execute(terminal, username, password);
            return ExitCodes.OK;
        }

        // pkg private for testing
        void execute(Terminal terminal, String username, String passwordStr) throws Exception {
            char[] password;
            if (passwordStr != null) {
                password = passwordStr.toCharArray();
                Validation.Error validationError = Validation.ESUsers.validatePassword(password);
                if (validationError != null) {
                    throw new UserError(ExitCodes.DATA_ERROR, "Invalid password..." + validationError);
                }
            } else {
                password = terminal.readSecret("Enter new password: ");
                Validation.Error validationError = Validation.ESUsers.validatePassword(password);
                if (validationError != null) {
                    throw new UserError(ExitCodes.DATA_ERROR, "Invalid password..." + validationError);
                }
                char[] retyped = terminal.readSecret("Retype new password: ");
                if (Arrays.equals(password, retyped) == false) {
                    throw new UserError(ExitCodes.DATA_ERROR, "Password mismatch");
                }
            }

            Settings esusersSettings = Realms.internalRealmSettings(env.settings(), ESUsersRealm.TYPE);
            Path file = FileUserPasswdStore.resolveFile(esusersSettings, env);
            FileAttributesChecker attributesChecker = new FileAttributesChecker(file);
            Map<String, char[]> users = new HashMap<>(FileUserPasswdStore.parseFile(file, null));
            if (users.containsKey(username) == false) {
                throw new UserError(ExitCodes.NO_USER, "User [" + username + "] doesn't exist");
            }
            Hasher hasher = Hasher.BCRYPT;
            users.put(username, hasher.hash(new SecuredString(password)));
            FileUserPasswdStore.writeFile(users, file);
            attributesChecker.check(terminal);
        }
    }

    static class RolesCommand extends Command {

        public static final Pattern ROLE_PATTERN = Pattern.compile("[\\w@-]+");

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
            terminal.println("The roles command allows to edit roles for an existing user.");
            terminal.println("corresponding roles. Alternatively you can also list just a");
            terminal.println("single users roles, if you do not specify the -a or the -r parameter.");
            terminal.println("");
        }

        @Override
        protected int execute(Terminal terminal, OptionSet options) throws Exception {
            String username = arguments.value(options);
            String addRoles = addOption.value(options);
            String removeRoles = removeOption.value(options);
            execute(terminal, username, addRoles, removeRoles);
            return ExitCodes.OK;
        }

        void execute(Terminal terminal, String username, String addRolesStr, String removeRolesStr) throws Exception {
            String[] addRoles = addRolesStr.split(",");
            String[] removeRoles = removeRolesStr.split(",");
            // check if just need to return data as no write operation happens
            // Nothing to add, just list the data for a username
            boolean readOnlyUserListing = removeRoles.length == 0 && addRoles.length == 0;
            if (readOnlyUserListing) {
                listUsersAndRoles(terminal, env, username);
                return;
            }

            // check for roles if they match
            String[] allRoles = ArrayUtils.concat(addRoles, removeRoles, String.class);
            for (String role : allRoles) {
                if (!ROLE_PATTERN.matcher(role).matches()) {
                    throw new UserError(ExitCodes.DATA_ERROR,
                        "Role name [" + role + "] is not valid. Please use lowercase and numbers only");
                }
            }

            Settings esusersSettings = Realms.internalRealmSettings(env.settings(), ESUsersRealm.TYPE);
            Path usersFile = FileUserPasswdStore.resolveFile(esusersSettings, env);
            Path rolesFile = FileUserRolesStore.resolveFile(esusersSettings, env);
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
            verifyRoles(terminal, env.settings(), env, addRoles);
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
            super("List existing users and their corresponding roles");
            this.env = env;
            this.arguments = parser.nonOptions("username");
        }

        @Override
        protected void printAdditionalHelp(Terminal terminal) {
            terminal.println("");
            terminal.println("");
        }

        @Override
        protected int execute(Terminal terminal, OptionSet options) throws Exception {
            String username = null;
            if (options.has(arguments)) {
                username = arguments.value(options);
            }
            listUsersAndRoles(terminal, env, username);
            return ExitCodes.OK;
        }
    }

    // pkg private for tests
    static void listUsersAndRoles(Terminal terminal, Environment env, String username) throws Exception {
        Settings esusersSettings = Realms.internalRealmSettings(env.settings(), ESUsersRealm.TYPE);
        Set<String> knownRoles = loadRoleNames(terminal, env.settings(), env);
        Path userRolesFilePath = FileUserRolesStore.resolveFile(esusersSettings, env);
        Map<String, String[]> userRoles = FileUserRolesStore.parseFile(userRolesFilePath, null);
        Path userFilePath = FileUserPasswdStore.resolveFile(esusersSettings, env);
        Set<String> users = FileUserPasswdStore.parseFile(userFilePath, null).keySet();

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
                    Path rolesFile = FileRolesStore.resolveFile(esusersSettings, env).toAbsolutePath();
                    terminal.println("");
                    terminal.println(" [*]   An unknown role. "
                        + "Please check [" + rolesFile.toAbsolutePath() + "] to see available roles");
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
                Path rolesFile = FileRolesStore.resolveFile(esusersSettings, env).toAbsolutePath();
                terminal.println("");
                terminal.println(" [*]   An unknown role. "
                    + "Please check [" + rolesFile.toAbsolutePath() + "] to see available roles");
            }
        }
    }

    private static Set<String> loadRoleNames(Terminal terminal, Settings settings, Environment env) {
        Path rolesFile = FileRolesStore.resolveFile(settings, env);
        try {
            return FileRolesStore.parseFileForRoleNames(rolesFile, null);
        } catch (Throwable t) {
            // if for some reason, parsing fails (malformatted perhaps) we just warn
            terminal.println(String.format(Locale.ROOT, "Warning:  Could not parse [%s] for roles verification. Please revise and fix it." +
                    " Nonetheless, the user will still be associated with all specified roles", rolesFile.toAbsolutePath()));
        }
        return null;
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

    private static void verifyRoles(Terminal terminal, Settings settings, Environment env, String[] roles) {
        Set<String> knownRoles = loadRoleNames(terminal, settings, env);
        Set<String> unknownRoles = Sets.difference(Sets.newHashSet(roles), knownRoles);
        if (!unknownRoles.isEmpty()) {
            Path rolesFile = FileRolesStore.resolveFile(settings, env);
            terminal.println(String.format(Locale.ROOT, "Warning: The following roles [%s] are unknown. Make sure to add them to the [%s]" +
                    " file. Nonetheless the user will still be associated with all specified roles",
                    Strings.collectionToCommaDelimitedString(unknownRoles), rolesFile.toAbsolutePath()));
        }
    }
}
