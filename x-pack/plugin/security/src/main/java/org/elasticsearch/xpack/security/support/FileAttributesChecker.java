/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.support;

import org.elasticsearch.cli.Terminal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;

/**
 * A utility for cli tools to capture file attributes
 * before writing files, and to warn if the permissions/group/owner changes.
 */
public class FileAttributesChecker {

    // the paths to check
    private final Path[] paths;

    // captured attributes for each path
    private final PosixFileAttributes[] attributes;

    /** Create a checker for the given paths, which will warn to the given terminal if changes are made. */
    public FileAttributesChecker(Path... paths) throws IOException {
        this.paths = paths;
        this.attributes = new PosixFileAttributes[paths.length];

        for (int i = 0; i < paths.length; ++i) {
            if (Files.exists(paths[i]) == false) continue; // missing file, so changes later don't matter
            PosixFileAttributeView view = Files.getFileAttributeView(paths[i], PosixFileAttributeView.class);
            if (view == null) continue; // not posix
            this.attributes[i] = view.readAttributes();
        }
    }

    /** Check if attributes of the paths have changed, warning to the given terminal if they have. */
    public void check(Terminal terminal) throws IOException {
        for (int i = 0; i < paths.length; ++i) {
            if (attributes[i] == null) {
                // we couldn't get attributes in setup, so we can't check them now
                continue;
            }

            PosixFileAttributeView view = Files.getFileAttributeView(paths[i], PosixFileAttributeView.class);
            PosixFileAttributes newAttributes = view.readAttributes();
            PosixFileAttributes oldAttributes = attributes[i];
            if (oldAttributes.permissions().equals(newAttributes.permissions()) == false) {
                terminal.errorPrintln(
                    Terminal.Verbosity.SILENT,
                    "WARNING: The file permissions of ["
                        + paths[i]
                        + "] have changed "
                        + "from ["
                        + PosixFilePermissions.toString(oldAttributes.permissions())
                        + "] "
                        + "to ["
                        + PosixFilePermissions.toString(newAttributes.permissions())
                        + "]"
                );
                terminal.errorPrintln(
                    Terminal.Verbosity.SILENT,
                    "Please ensure that the user account running Elasticsearch has read access to this file!"
                );
            }
            if (oldAttributes.owner().getName().equals(newAttributes.owner().getName()) == false) {
                terminal.errorPrintln(
                    Terminal.Verbosity.SILENT,
                    "WARNING: Owner of file ["
                        + paths[i]
                        + "] "
                        + "used to be ["
                        + oldAttributes.owner().getName()
                        + "], "
                        + "but now is ["
                        + newAttributes.owner().getName()
                        + "]"
                );
            }
            if (oldAttributes.group().getName().equals(newAttributes.group().getName()) == false) {
                terminal.errorPrintln(
                    Terminal.Verbosity.SILENT,
                    "WARNING: Group of file ["
                        + paths[i]
                        + "] "
                        + "used to be ["
                        + oldAttributes.group().getName()
                        + "], "
                        + "but now is ["
                        + newAttributes.group().getName()
                        + "]"
                );
            }
        }
    }
}
