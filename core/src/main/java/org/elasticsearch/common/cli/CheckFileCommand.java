/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.cli;

import com.google.common.collect.Maps;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Map;
import java.util.Set;

/**
 * A helper command that checks if configured paths have been changed when running a CLI command.
 * It is only executed in case of specified paths by the command and if the paths underlying filesystem
 * supports posix permissions.
 *
 * If this is the case, a warn message is issued whenever an owner, a group or the file permissions is changed by
 * the command being executed and not configured back to its prior state, which should be the task of the command
 * being executed.
 *
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

        if (paths == null || paths.length == 0) {
            return doExecute(settings, env);
        }

        Map<Path, Set<PosixFilePermission>> permissions = Maps.newHashMapWithExpectedSize(paths.length);
        Map<Path, String> owners = Maps.newHashMapWithExpectedSize(paths.length);
        Map<Path, String> groups = Maps.newHashMapWithExpectedSize(paths.length);

        if (paths != null && paths.length > 0) {
            for (Path path : paths) {
                try {
                    boolean supportsPosixPermissions = Files.getFileStore(path).supportsFileAttributeView(PosixFileAttributeView.class);
                    if (supportsPosixPermissions) {
                        PosixFileAttributes attributes = Files.readAttributes(path, PosixFileAttributes.class);
                        permissions.put(path, attributes.permissions());
                        owners.put(path, attributes.owner().getName());
                        groups.put(path, attributes.group().getName());
                    }
                } catch (IOException e) {
                    // silently swallow if not supported, no need to log things
                }
            }
        }

        CliTool.ExitStatus status = doExecute(settings, env);

        // check if permissions differ
        for (Map.Entry<Path, Set<PosixFilePermission>> entry : permissions.entrySet()) {
            if (!Files.exists(entry.getKey())) {
                continue;
            }

            Set<PosixFilePermission> permissionsBeforeWrite = entry.getValue();
            Set<PosixFilePermission> permissionsAfterWrite = Files.getPosixFilePermissions(entry.getKey());
            if (!permissionsBeforeWrite.equals(permissionsAfterWrite)) {
                terminal.printWarn("The file permissions of [%s] have changed from [%s] to [%s]",
                        entry.getKey(), PosixFilePermissions.toString(permissionsBeforeWrite), PosixFilePermissions.toString(permissionsAfterWrite));
                terminal.printWarn("Please ensure that the user account running Elasticsearch has read access to this file!");
            }
        }

        // check if owner differs
        for (Map.Entry<Path, String> entry : owners.entrySet()) {
            if (!Files.exists(entry.getKey())) {
                continue;
            }

            String ownerBeforeWrite = entry.getValue();
            String ownerAfterWrite = Files.getOwner(entry.getKey()).getName();
            if (!ownerAfterWrite.equals(ownerBeforeWrite)) {
                terminal.printWarn("WARN: Owner of file [%s] used to be [%s], but now is [%s]", entry.getKey(), ownerBeforeWrite, ownerAfterWrite);
            }
        }

        // check if group differs
        for (Map.Entry<Path, String> entry : groups.entrySet()) {
            if (!Files.exists(entry.getKey())) {
                continue;
            }

            String groupBeforeWrite = entry.getValue();
            String groupAfterWrite = Files.readAttributes(entry.getKey(), PosixFileAttributes.class).group().getName();
            if (!groupAfterWrite.equals(groupBeforeWrite)) {
                terminal.printWarn("WARN: Group of file [%s] used to be [%s], but now is [%s]", entry.getKey(), groupBeforeWrite, groupAfterWrite);
            }
        }

        return status;
    }
}
