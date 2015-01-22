/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.common.cli;

import com.google.common.collect.Sets;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.GroupPrincipal;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.UserPrincipal;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class CheckFileCommandTests extends ElasticsearchTestCase {

    private CliToolTestCase.CaptureOutputTerminal captureOutputTerminal = new CliToolTestCase.CaptureOutputTerminal();

    private Configuration jimFsConfiguration = Configuration.unix().toBuilder().setAttributeViews("basic", "owner", "posix", "unix").build();
    private Configuration jimFsConfigurationWithoutPermissions = randomBoolean() ? Configuration.unix().toBuilder().setAttributeViews("basic").build() : Configuration.windows();

    @Test
    public void testThatCommandLogsErrorMessageOnFail() throws Exception {
        executeCommand(jimFsConfiguration, new PermissionCheckFileCommand(captureOutputTerminal, true));
        assertThat(captureOutputTerminal.getTerminalOutput(), hasItem(containsString("Please ensure correct permissions")));
    }

    @Test
    public void testThatCommandLogsNothingOnSuccess() throws Exception {
        executeCommand(jimFsConfiguration, new PermissionCheckFileCommand(captureOutputTerminal, false));
        assertThat(captureOutputTerminal.getTerminalOutput(), hasSize(0));
    }

    @Test
    public void testThatCommandLogsNothingIfFilesystemDoesNotSupportPermissions() throws Exception {
        executeCommand(jimFsConfigurationWithoutPermissions, new PermissionCheckFileCommand(captureOutputTerminal, false));
        assertThat(captureOutputTerminal.getTerminalOutput(), hasSize(0));
    }

    @Test
    public void testThatCommandLogsOwnerChange() throws Exception {
        executeCommand(jimFsConfiguration, new OwnerCheckFileCommand(captureOutputTerminal, true));
        assertThat(captureOutputTerminal.getTerminalOutput(), hasItem(allOf(containsString("Owner of file ["), containsString("] used to be ["), containsString("], but now is ["))));
    }

    @Test
    public void testThatCommandLogsNothingIfOwnerIsNotChanged() throws Exception {
        executeCommand(jimFsConfiguration, new OwnerCheckFileCommand(captureOutputTerminal, false));
        assertThat(captureOutputTerminal.getTerminalOutput(), hasSize(0));
    }

    @Test
    public void testThatCommandLogsNothingIfFileSystemDoesNotSupportOwners() throws Exception {
        executeCommand(jimFsConfigurationWithoutPermissions, new OwnerCheckFileCommand(captureOutputTerminal, false));
        assertThat(captureOutputTerminal.getTerminalOutput(), hasSize(0));
    }

    @Test
    public void testThatCommandLogsIfGroupChanges() throws Exception {
        executeCommand(jimFsConfiguration, new GroupCheckFileCommand(captureOutputTerminal, true));
        assertThat(captureOutputTerminal.getTerminalOutput(), hasItem(allOf(containsString("Group of file ["), containsString("] used to be ["), containsString("], but now is ["))));
    }

    @Test
    public void testThatCommandLogsNothingIfGroupIsNotChanged() throws Exception {
        executeCommand(jimFsConfiguration, new GroupCheckFileCommand(captureOutputTerminal, false));
        assertThat(captureOutputTerminal.getTerminalOutput(), hasSize(0));
    }

    @Test
    public void testThatCommandLogsNothingIfFileSystemDoesNotSupportGroups() throws Exception {
        executeCommand(jimFsConfigurationWithoutPermissions, new GroupCheckFileCommand(captureOutputTerminal, false));
        assertThat(captureOutputTerminal.getTerminalOutput(), hasSize(0));
    }

    private void executeCommand(Configuration configuration, AbstractTestCheckFileCommand command) throws Exception {
        try (FileSystem fs = Jimfs.newFileSystem(configuration)) {
            command.execute(fs);
        }
    }

    abstract class AbstractTestCheckFileCommand extends CheckFileCommand {

        protected final boolean enabled;
        protected FileSystem fs;
        protected Path[] paths;

        public AbstractTestCheckFileCommand(Terminal terminal, boolean enabled) throws IOException {
            super(terminal);
            this.enabled = enabled;
        }

        public CliTool.ExitStatus execute(FileSystem fs) throws Exception {
            this.fs = fs;
            this.paths = new Path[] { writePath(fs, "p1", "anything"), writePath(fs, "p2", "anything"), writePath(fs, "p3", "anything") };
            return super.execute(ImmutableSettings.EMPTY, new Environment(ImmutableSettings.EMPTY));
        }

        private Path writePath(FileSystem fs, String name, String content) throws IOException {
            Path path = fs.getPath(name);
            Files.write(path, content.getBytes(Charsets.UTF_8));
            return path;
        }

        @Override
        protected Path[] pathsForPermissionsCheck(Settings settings, Environment env) {
            return paths;
        }
    }

    /**
     * command that changes permissions from a file if enabled
     */
    class PermissionCheckFileCommand extends AbstractTestCheckFileCommand {

        public PermissionCheckFileCommand(Terminal terminal, boolean enabled) throws IOException {
            super(terminal, enabled);
        }

        @Override
        public CliTool.ExitStatus doExecute(Settings settings, Environment env) throws Exception {
            if (enabled) {
                int randomInt = randomInt(paths.length - 1);
                Files.setPosixFilePermissions(paths[randomInt], Sets.newHashSet(PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.OTHERS_EXECUTE, PosixFilePermission.GROUP_EXECUTE));
            }
            return CliTool.ExitStatus.OK;
        }

    }

    /**
     * command that changes the owner of a file if enabled
     */
    class OwnerCheckFileCommand extends AbstractTestCheckFileCommand {

        public OwnerCheckFileCommand(Terminal terminal, boolean enabled) throws IOException {
            super(terminal, enabled);
        }

        @Override
        public CliTool.ExitStatus doExecute(Settings settings, Environment env) throws Exception {
            if (enabled) {
                int randomInt = randomInt(paths.length - 1);
                UserPrincipal owner = fs.getUserPrincipalLookupService().lookupPrincipalByName(randomAsciiOfLength(10));
                Files.setOwner(paths[randomInt], owner);
            }
            return CliTool.ExitStatus.OK;
        }
    }

    /**
     * command that changes the group of a file if enabled
     */
    class GroupCheckFileCommand extends AbstractTestCheckFileCommand {

        public GroupCheckFileCommand(Terminal terminal, boolean enabled) throws IOException {
            super(terminal, enabled);
        }

        @Override
        public CliTool.ExitStatus doExecute(Settings settings, Environment env) throws Exception {
            if (enabled) {
                int randomInt = randomInt(paths.length - 1);
                GroupPrincipal groupPrincipal = fs.getUserPrincipalLookupService().lookupPrincipalByGroupName(randomAsciiOfLength(10));
                Files.getFileAttributeView(paths[randomInt], PosixFileAttributeView.class).setGroup(groupPrincipal);
            }
            return CliTool.ExitStatus.OK;
        }
    }
}
