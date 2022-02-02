/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.transform;

import org.gradle.api.artifacts.transform.InputArtifact;
import org.gradle.api.artifacts.transform.TransformAction;
import org.gradle.api.artifacts.transform.TransformOutputs;
import org.gradle.api.artifacts.transform.TransformParameters;
import org.gradle.api.file.FileSystemLocation;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.internal.UncheckedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Function;

public interface UnpackTransform extends TransformAction<UnpackTransform.Parameters> {

    interface Parameters extends TransformParameters {
        @Input
        @Optional
        String getTrimmedPrefixPattern();

        void setTrimmedPrefixPattern(String pattern);

        @Input
        @Optional
        List<String> getKeepStructureFor();

        /**
         * Mark output as handled like a filetree meaning that
         * each file will be part of the output and not the singular ouptut directory.
         * */
        @Input
        boolean getAsFiletreeOutput();

        void setAsFiletreeOutput(boolean asFiletreeOutput);

        @Internal
        File getTargetDirectory();

        void setTargetDirectory(File targetDirectory);

        void setKeepStructureFor(List<String> pattern);
    }

    @PathSensitive(PathSensitivity.NAME_ONLY)
    @InputArtifact
    Provider<FileSystemLocation> getArchiveFile();

    @Override
    default void transform(TransformOutputs outputs) {
        File archiveFile = getArchiveFile().get().getAsFile();
        File extractedDir = outputs.dir(archiveFile.getName());
        try {
            Logging.getLogger(UnpackTransform.class)
                .info("Unpacking " + archiveFile.getName() + " using " + getClass().getSimpleName() + ".");
            unpack(archiveFile, extractedDir, outputs, getParameters().getAsFiletreeOutput());
        } catch (IOException e) {
            throw UncheckedException.throwAsUncheckedException(e);
        }
    }

    void unpack(File archiveFile, File targetDir, TransformOutputs outputs, boolean asFiletreeOutput) throws IOException;

    default Function<String, Path> pathResolver() {
        List<String> keepPatterns = getParameters().getKeepStructureFor();
        String trimmedPrefixPattern = getParameters().getTrimmedPrefixPattern();
        return trimmedPrefixPattern != null ? (i) -> trimArchiveExtractPath(keepPatterns, trimmedPrefixPattern, i) : (i) -> Path.of(i);
    }

    /*
     * We want to be able to trim off certain prefixes when transforming archives.
     *
     * E.g We want to remove up to the and including the jdk-.* relative paths. That is a JDK archive is structured as:
     *   jdk-12.0.1/
     *   jdk-12.0.1/Contents
     *   ...
     *
     * and we want to remove the leading jdk-12.0.1. Note however that there could also be a leading ./ as in
     *   ./
     *   ./jdk-12.0.1/
     *   ./jdk-12.0.1/Contents
     *
     * so we account for this and search the path components until we find the jdk-12.0.1, and strip the leading components.
     *
     * Azul jdk linux arm distribution is packaged differently and missed the in between layer.
     * For now we just resolve this by having a keep pattern for distributions that root folder name
     * matches certain pattern giving us a hint on which distro we're dealing with.
     */
    static Path trimArchiveExtractPath(List<String> keepPatterns, String ignoredPattern, String relativePath) {
        final Path entryName = Paths.get(relativePath);
        // Do we want to keep the origin packaging just without the root folder?
        if (keepPatterns != null && keepPatterns.stream().anyMatch(keepPattern -> entryName.getName(0).toString().matches(keepPattern))) {
            if (entryName.getNameCount() == 1) {
                return null;
            } else {
                return entryName.subpath(1, entryName.getNameCount());
            }
        }

        int index = 0;
        for (; index < entryName.getNameCount(); index++) {
            if (entryName.getName(index).toString().matches(ignoredPattern)) {
                break;
            }
        }
        if (index + 1 >= entryName.getNameCount()) {
            // this happens on the top-level directories in the archive, which we are removing
            return null;
        }
        // finally remove the top-level directories from the output path
        return entryName.subpath(index + 1, entryName.getNameCount());
    }
}
