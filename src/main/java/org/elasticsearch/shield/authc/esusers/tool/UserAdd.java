/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esusers.tool;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.shield.authc.esusers.FileUserPasswdStore;
import org.elasticsearch.shield.authc.esusers.FileUserRolesStore;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.support.CmdLineTool;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class UserAdd extends CmdLineTool {

    public static void main(String[] args) throws Exception {
        new UserAdd().execute(args);
    }

    public UserAdd() {
        super("useradd",
                option("p", "password", "The user password").hasArg(true).required(true),
                option("r", "roles", "Comma-separated list of the roles of the user").hasArg(true).required(true),
                option("h", "help", "Prints usage help").hasArg(false).required(false)
        );
    }

    public void run(CommandLine cli) throws Exception {

        if (cli.getArgs().length == 0) {
            terminal.println("username is missing");
            printUsage();
            exit(ExitStatus.USAGE);
        }

        String username = cli.getArgs()[0];


        char[] password;
        String passwordStr = cli.getOptionValue("password");
        if (passwordStr != null) {
            password = passwordStr.toCharArray();
        } else {
            password = terminal.readPassword("Enter new password: ");
            char[] retyped = terminal.readPassword("Retype new password: ");
            if (!Arrays.equals(password, retyped)) {
                terminal.print("Password mismatch");
                exit(ExitStatus.USAGE);
            }
        }

        String[] roles = null;
        String rolesCsv = cli.getOptionValue("roles");
        if (rolesCsv != null) {
            roles = rolesCsv.split(",");
        }
        addUser(username, password, roles);
    }

    private void addUser(String username, char[] passwd, String[] roles) {
        Path file = FileUserPasswdStore.resolveFile(settings, env);
        Map<String, char[]> users = FileUserPasswdStore.parseFile(file, null);
        if (users == null) {
            // file doesn't exist so we just create a new file
            users = new HashMap<>();
        }
        if (users.containsKey(username)) {
            terminal.println("User [{}] already exists", username);
            exit(ExitStatus.CODE_ERROR);
        }
        Hasher hasher = Hasher.HTPASSWD;
        users.put(username, hasher.hash(passwd));
        FileUserPasswdStore.writeFile(users, file);

        file = FileUserRolesStore.resolveFile(settings, env);
        Map<String, String[]> userRoles = FileUserRolesStore.parseFile(file, null);
        if (userRoles == null) {
            // file doesn't exist, so we just create a new file
            userRoles = new HashMap<>();
        }
        userRoles.put(username, roles);
        FileUserRolesStore.writeFile(userRoles, file);
    }

}
