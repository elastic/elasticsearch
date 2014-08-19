/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esusers.tool;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.base.Joiner;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolConfig;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.cli.commons.CommandLine;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.collect.ObjectArrays;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.authc.esusers.FileUserPasswdStore;
import org.elasticsearch.shield.authc.esusers.FileUserRolesStore;
import org.elasticsearch.shield.authc.support.Hasher;

import java.nio.file.Path;
import java.util.*;
import java.util.regex.Pattern;

import static org.elasticsearch.common.cli.CliToolConfig.Builder.cmd;
import static org.elasticsearch.common.cli.CliToolConfig.Builder.option;

/**
 *
 */
public class ESUsersTool extends CliTool {

    private static final CliToolConfig CONFIG = CliToolConfig.config("esusers", ESUsersTool.class)
            .cmds(Useradd.CMD, Userdel.CMD, Passwd.CMD, Roles.CMD, ListUsersAndRoles.CMD)
            .build();

    public static void main(String[] args) {
        new ESUsersTool().execute(args);
    }

    public ESUsersTool() {
        super(CONFIG);
    }

    public ESUsersTool(Terminal terminal) {
        super(CONFIG, terminal);
    }

    @Override
    protected Command parse(String cmdName, CommandLine cli) throws Exception {
        switch (cmdName.toLowerCase(Locale.ROOT)) {
            case Useradd.NAME:
                return Useradd.parse(terminal, cli);
            case Userdel.NAME:
                return Userdel.parse(terminal, cli);
            case Passwd.NAME:
                return Passwd.parse(terminal, cli);
            case ListUsersAndRoles.NAME:
                return ListUsersAndRoles.parse(terminal, cli);
            case Roles.NAME:
                return Roles.parse(terminal, cli);
            default:
                assert false : "should never get here, if the user enters an unknown command, an error message should be shown before parse is called";
                return null;
        }
    }

    static class Useradd extends CliTool.Command {

        private static final String NAME = "useradd";

        private static final CliToolConfig.Cmd CMD = cmd(NAME, Useradd.class)
                .options(
                        option("p", "password").hasArg(false).required(false),
                        option("r", "roles").hasArg(false).required(false))
                .build();

        public static Command parse(Terminal terminal, CommandLine cli) {
            if (cli.getArgs().length == 0) {
                return exitCmd(ExitStatus.USAGE, terminal, "username is missing");
            }

            String username = cli.getArgs()[0];

            char[] password;
            String passwordStr = cli.getOptionValue("password");
            if (passwordStr != null) {
                password = passwordStr.toCharArray();
            } else {
                password = terminal.readSecret("Enter new password: ");
                char[] retyped = terminal.readSecret("Retype new password: ");
                if (!Arrays.equals(password, retyped)) {
                    return exitCmd(ExitStatus.USAGE, terminal, "Password mismatch");
                }
            }

            String rolesCsv = cli.getOptionValue("roles");
            String[] roles = (rolesCsv != null) ? rolesCsv.split(",") : Strings.EMPTY_ARRAY;
            return new Useradd(terminal, username, password, roles);
        }

        final String username;
        final char[] passwd;
        final String[] roles;

        Useradd(Terminal terminal, String username, char[] passwd, String... roles) {
            super(terminal);
            this.username = username;
            this.passwd = passwd;
            this.roles = roles;
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {
            Path file = FileUserPasswdStore.resolveFile(settings, env);
            Map<String, char[]> users = new HashMap<>(FileUserPasswdStore.parseFile(file, null));
            if (users == null) {
                // file doesn't exist so we just create a new file
                users = new HashMap<>();
            }
            if (users.containsKey(username)) {
                terminal.println("User [%s] already exists", username);
                return ExitStatus.CODE_ERROR;
            }
            Hasher hasher = Hasher.HTPASSWD;
            users.put(username, hasher.hash(passwd));
            FileUserPasswdStore.writeFile(users, file);


            file = FileUserRolesStore.resolveFile(settings, env);
            Map<String, String[]> userRoles = new HashMap<>(FileUserRolesStore.parseFile(file, null));
            if (userRoles == null) {
                // file doesn't exist, so we just create a new file
                userRoles = new HashMap<>();
            }
            userRoles.put(username, roles);
            FileUserRolesStore.writeFile(userRoles, file);
            return ExitStatus.OK;
        }
    }

    static class Userdel extends CliTool.Command {

        private static final String NAME = "userdel";

        private static final CliToolConfig.Cmd CMD = cmd(NAME, Userdel.class).build();

        public static Command parse(Terminal terminal, CommandLine cli) {
            if (cli.getArgs().length == 0) {
                return exitCmd(ExitStatus.USAGE, terminal, "username is missing");
            }

            String username = cli.getArgs()[0];
            return new Userdel(terminal, username);
        }

        final String username;

        Userdel(Terminal terminal, String username) {
            super(terminal);
            this.username = username;
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {
            Path file = FileUserPasswdStore.resolveFile(settings, env);
            Map<String, char[]> users = new HashMap<>(FileUserPasswdStore.parseFile(file, null));
            if (users != null) {
                char[] passwd = users.remove(username);
                if (passwd != null) {
                    FileUserPasswdStore.writeFile(users, file);
                } else {
                    terminal.println("Warning: users file [%s] did not contain password entry for user [%s]", file.toAbsolutePath(), username);
                }
            }

            file = FileUserRolesStore.resolveFile(settings, env);
            Map<String, String[]> userRoles = new HashMap<>(FileUserRolesStore.parseFile(file, null));
            if (userRoles != null) {
                String[] roles = userRoles.remove(username);
                if (roles != null) {
                    FileUserRolesStore.writeFile(userRoles, file);
                } else {
                    terminal.println("Warning: users_roles file [%s] did not contain roles entry for user [%s]", file.toAbsolutePath(), username);
                }
            }

            return ExitStatus.OK;
        }
    }

    static class Passwd extends CliTool.Command {

        private static final String NAME = "passwd";

        private static final CliToolConfig.Cmd CMD = cmd(NAME, Passwd.class)
                .options(option("p", "password").hasArg(false).required(false))
                .build();

        public static Command parse(Terminal terminal, CommandLine cli) {
            if (cli.getArgs().length == 0) {
                return exitCmd(ExitStatus.USAGE, terminal, "username is missing");
            }

            String username = cli.getArgs()[0];

            char[] password;
            String passwordStr = cli.getOptionValue("password");
            if (passwordStr != null) {
                password = passwordStr.toCharArray();
            } else {
                password = terminal.readSecret("Enter new password: ");
                char[] retyped = terminal.readSecret("Retype new password: ");
                if (!Arrays.equals(password, retyped)) {
                    return exitCmd(ExitStatus.USAGE, terminal, "Password mismatch");
                }
            }
            return new Passwd(terminal, username, password);
        }

        final String username;
        final char[] passwd;

        Passwd(Terminal terminal, String username, char[] passwd) {
            super(terminal);
            this.username = username;
            this.passwd = passwd;
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {
            Path file = FileUserPasswdStore.resolveFile(settings, env);
            Map<String, char[]> users = new HashMap<>(FileUserPasswdStore.parseFile(file, null));
            if (users == null) {
                // file doesn't exist so we just create a new file
                users = new HashMap<>();
            }
            if (!users.containsKey(username)) {
                terminal.println("User [%s] doesn't exist", username);
                return ExitStatus.NO_USER;
            }
            Hasher hasher = Hasher.HTPASSWD;
            users.put(username, hasher.hash(passwd));
            FileUserPasswdStore.writeFile(users, file);
            return ExitStatus.OK;
        }
    }


    static class Roles extends CliTool.Command {

        private static final String NAME = "roles";

        private static final CliToolConfig.Cmd CMD = cmd(NAME, Roles.class)
                .options(
                        option("a", "add").hasArg(true).required(false),
                        option("r", "remove").hasArg(true).required(false))
                .build();

        public static Command parse(Terminal terminal, CommandLine cli) {
            if (cli.getArgs().length == 0) {
                return exitCmd(ExitStatus.USAGE, terminal, "username is missing");
            }

            String username = cli.getArgs()[0];
            String addRolesCsv = cli.getOptionValue("add");
            String[] addRoles = (addRolesCsv != null) ? addRolesCsv.split(",") : Strings.EMPTY_ARRAY;
            String removeRolesCsv = cli.getOptionValue("remove");
            String[] removeRoles = (removeRolesCsv != null) ? removeRolesCsv.split(",") : Strings.EMPTY_ARRAY;

            return new Roles(terminal, username, addRoles, removeRoles);
        }

        public static final Pattern ROLE_PATTERN = Pattern.compile("[\\w@-]+");
        final String username;
        final String[] addRoles;
        final String[] removeRoles;

        public Roles(Terminal terminal, String username, String[] addRoles, String[] removeRoles) {
            super(terminal);
            this.username = username;
            this.addRoles = addRoles;
            this.removeRoles = removeRoles;
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {
            // check if just need to return data as no write operation happens
            // Nothing to add, just list the data for a username
            boolean readOnlyUserListing = removeRoles.length == 0 && addRoles.length == 0;
            if (readOnlyUserListing) {
                return new ListUsersAndRoles(terminal, username).execute(settings, env);
            }


            // check for roles if they match
            String[] allRoles = ObjectArrays.concat(addRoles, removeRoles, String.class);
            for (String role : allRoles) {
                if (!ROLE_PATTERN.matcher(role).matches()) {
                    terminal.println("Role name [%s] is not valid. Please use lowercase and numbers only", role);
                    return ExitStatus.DATA_ERROR;
                }
            }

            Path file = FileUserRolesStore.resolveFile(settings, env);
            Map<String, String[]> userRoles = FileUserRolesStore.parseFile(file, null);

            if (!userRoles.containsKey(username)) {
                terminal.println("User [%s] doesn't exist", username);
                return ExitStatus.NO_USER;
            }

            List<String> roles = Lists.newArrayList(userRoles.get(username));;
            roles.addAll(Arrays.asList(addRoles));
            roles.removeAll(Arrays.asList(removeRoles));

            Map<String, String[]> userRolesToWrite = Maps.newHashMapWithExpectedSize(userRoles.size());
            userRolesToWrite.putAll(userRoles);
            userRolesToWrite.put(username, Sets.newLinkedHashSet(roles).toArray(new String[]{}));
            FileUserRolesStore.writeFile(userRolesToWrite, file);

            return ExitStatus.OK;
        }
    }

    static class ListUsersAndRoles extends CliTool.Command {

        private static final String NAME = "list";

        private static final CliToolConfig.Cmd CMD = cmd(NAME, Useradd.class).build();

        public static Command parse(Terminal terminal, CommandLine cli) {
            String username = (cli.getArgs().length > 0) ? cli.getArgs()[0] : null;
            return new ListUsersAndRoles(terminal, username);
        }

        String username;

        public ListUsersAndRoles(Terminal terminal, String username) {
            super(terminal);
            this.username = username;
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {
            Path userRolesFilePath = FileUserRolesStore.resolveFile(settings, env);
            Map<String, String[]> userRoles = FileUserRolesStore.parseFile(userRolesFilePath, null);
            Path userFilePath = FileUserPasswdStore.resolveFile(settings, env);
            Set<String> users = FileUserPasswdStore.parseFile(userFilePath, null).keySet();

            if (username != null) {
                if (!users.contains(username)) {
                    terminal.println("User [%s] doesn't exist", username);
                    return ExitStatus.NO_USER;
                }

                if (userRoles.containsKey(username)) {
                    terminal.println("%-15s: %s", username, Joiner.on(",").useForNull("-").join(userRoles.get(username)));
                } else {
                    terminal.println("%-15s: -", username);
                }
            } else {
                for (Map.Entry<String, String[]> entry : userRoles.entrySet()) {
                    terminal.println("%-15s: %s", entry.getKey(), Joiner.on(",").join(entry.getValue()));
                }
                // list users without roles
                Set<String> usersWithoutRoles = Sets.newHashSet(users);
                if (usersWithoutRoles.removeAll(userRoles.keySet())) {
                    for (String user : usersWithoutRoles) {
                        terminal.println("%-15s: -", user);
                    }
                }
            }

            return ExitStatus.OK;
        }
    }
}