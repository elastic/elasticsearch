/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;

import java.io.File;
import java.util.List;

import javax.inject.Inject;

/**
 * Extracts a deb or rpm package with the package format's own tool, as a sanity check that the package is readable by
 * it, and so that the installed files can be checked.
 *
 * <p>The rpm package is installed into a throw-away rpm database with its scripts and dependencies disabled and every
 * path relocated into the destination directory. Signatures are not verified because packages are signed after they
 * are built, and recent rpm versions refuse to install unsigned packages by default.
 */
public abstract class ExtractPackageTask extends DefaultTask {

    /**
     * The package to extract.
     */
    @InputFile
    @PathSensitive(PathSensitivity.NONE)
    public abstract RegularFileProperty getPackageFile();

    @Input
    public abstract Property<PackageMetadata.Type> getPackageType();

    /**
     * The absolute path of {@code dpkg-deb} or {@code rpm}, matching {@link #getPackageType()}.
     */
    @Input
    public abstract Property<String> getExecutable();

    /**
     * The directory the package's files are extracted into, using their install paths relative to it.
     */
    @OutputDirectory
    public abstract DirectoryProperty getDestinationDir();

    @Inject
    protected abstract ExecOperations getExecOperations();

    @Inject
    protected abstract FileSystemOperations getFileSystemOperations();

    @TaskAction
    public void extract() {
        File destination = getDestinationDir().get().getAsFile();
        File rpmDatabase = new File(getTemporaryDir(), "rpm-database");
        getFileSystemOperations().delete(spec -> spec.delete(destination, rpmDatabase));
        destination.mkdirs();
        List<String> commandLine = commandLine(
            getPackageType().get(),
            getExecutable().get(),
            getPackageFile().get().getAsFile(),
            destination,
            rpmDatabase
        );
        PackageTools.run(getExecOperations(), commandLine);
    }

    static List<String> commandLine(PackageMetadata.Type type, String executable, File packageFile, File destination, File rpmDatabase) {
        return switch (type) {
            case DEB -> List.of(executable, "-x", packageFile.getAbsolutePath(), destination.getAbsolutePath());
            case RPM -> List.of(
                executable,
                "--badreloc",
                "--ignorearch",
                "--ignoreos",
                "--nodeps",
                "--noscripts",
                "--notriggers",
                "--nosignature",
                "--dbpath",
                rpmDatabase.getAbsolutePath(),
                "--relocate",
                "/=" + destination.getAbsolutePath(),
                "-i",
                packageFile.getAbsolutePath()
            );
        };
    }
}
