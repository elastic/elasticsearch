/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.common.cli;

import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Map;
import java.util.Set;

/**
 * helper command to check if file permissions or owner got changed by the command being executed
 */
public abstract class CheckFileCommand extends CliTool.Command {

    public CheckFileCommand(Terminal terminal) {
        super(terminal);
    }

    /**
     * abstract method, which should implement the same logic as CliTool.Command.execute(), but is wrapped
     */
    public abstract CliTool.ExitStatus doExecute(Settings settings, Environment env) throws Exception;

    /**
     * Returns the array of paths, that should be checked if the permissions, user or groups have changed
     * before and after execution of the command
     *
     */
    protected abstract Path[] pathsForPermissionsCheck(Settings settings, Environment env) throws Exception;

    @Override
    public CliTool.ExitStatus execute(Settings settings, Environment env) throws Exception {
        Path[] paths = pathsForPermissionsCheck(settings, env);

        Map<Path, Set<PosixFilePermission>> permissions = Maps.newHashMapWithExpectedSize(paths.length);
        Map<Path, String> owners = Maps.newHashMapWithExpectedSize(paths.length);
        Map<Path, String> groups = Maps.newHashMapWithExpectedSize(paths.length);

        if (paths != null && paths.length > 0) {
            for (Path path : paths) {
                try {
                    boolean supportsPosixPermissions = Files.getFileStore(path).supportsFileAttributeView(PosixFileAttributeView.class);
                    if (supportsPosixPermissions) {
                        permissions.put(path, Files.getPosixFilePermissions(path));
                        owners.put(path, Files.getOwner(path).getName());
                        groups.put(path, Files.readAttributes(path, PosixFileAttributes.class).group().getName());
                    }
                } catch (IOException e) {
                    // silently swallow if not supported, no need to log things
                }
            }
        }

        CliTool.ExitStatus status = doExecute(settings, env);

        // check if permissions differ
        for (Map.Entry<Path, Set<PosixFilePermission>> entry : permissions.entrySet()) {
            Set<PosixFilePermission> permissionsBeforeWrite = entry.getValue();
            Set<PosixFilePermission> permissionsAfterWrite = Files.getPosixFilePermissions(entry.getKey());
            if (!permissionsBeforeWrite.equals(permissionsAfterWrite)) {
                terminal.printError("Permissions of [%s] differ after write. Please ensure correct permissions before going on!", entry.getKey());
            }
        }

        // check if owner differs
        for (Map.Entry<Path, String> entry : owners.entrySet()) {
            String ownerBeforeWrite = entry.getValue();
            String ownerAfterWrite = Files.getOwner(entry.getKey()).getName();
            if (!ownerAfterWrite.equals(ownerBeforeWrite)) {
                terminal.printError("Owner of file [%s] used to be [%s], but now is [%s]", entry.getKey(), ownerBeforeWrite, ownerAfterWrite);
            }
        }

        // check if group differs
        for (Map.Entry<Path, String> entry : groups.entrySet()) {
            String groupBeforeWrite = entry.getValue();
            String groupAfterWrite = Files.readAttributes(entry.getKey(), PosixFileAttributes.class).group().getName();
            if (!groupAfterWrite.equals(groupBeforeWrite)) {
                terminal.printError("Group of file [%s] used to be [%s], but now is [%s]", entry.getKey(), groupBeforeWrite, groupAfterWrite);
            }
        }

        return status;
    }
}
