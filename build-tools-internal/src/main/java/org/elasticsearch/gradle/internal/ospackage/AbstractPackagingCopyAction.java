/*
 * @notice
 * Copyright 2011-2019 the original author or authors.
 * Modifications copyright (C) 2026 Elasticsearch B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file is derived from the nebula gradle-ospackage-plugin
 * (https://github.com/nebula-plugins/gradle-ospackage-plugin), reduced to the
 * functionality used by the Elasticsearch build and rewritten in Java.
 */

package org.elasticsearch.gradle.internal.ospackage;

import org.gradle.api.file.CopySpec;
import org.gradle.api.file.FileCopyDetails;
import org.gradle.api.internal.file.CopyActionProcessingStreamAction;
import org.gradle.api.internal.file.copy.CopyAction;
import org.gradle.api.internal.file.copy.CopyActionProcessingStream;
import org.gradle.api.internal.file.copy.CopySpecResolver;
import org.gradle.api.internal.file.copy.DefaultCopySpec;
import org.gradle.api.internal.file.copy.DefaultFileCopyDetails;
import org.gradle.api.internal.file.copy.FileCopyDetailsInternal;
import org.gradle.api.tasks.WorkResult;
import org.gradle.api.tasks.WorkResults;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Objects;

/**
 * Visits the resolved copy stream of a packaging task and forwards files, directories and package
 * relationships to a package-format specific backend (redline for rpm, jdeb for deb).
 */
public abstract class AbstractPackagingCopyAction<T extends SystemPackagingTask> implements CopyAction {

    protected final T task;
    protected File tempDir;

    protected AbstractPackagingCopyAction(T task) {
        this.task = task;
    }

    @Override
    public WorkResult execute(CopyActionProcessingStream stream) {
        try {
            startVisit();
            stream.process(new StreamAction());
            endVisit();
        } catch (Exception e) {
            if (e instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new RuntimeException(e);
        }
        return WorkResults.didWork(true);
    }

    private class StreamAction implements CopyActionProcessingStreamAction {
        @Override
        public void processFile(FileCopyDetailsInternal details) {
            CopySpec spec = extractSpec(details); // can be null
            if (details.isDirectory()) {
                visitDir(details, spec);
            } else {
                visitFile(details, spec);
            }
        }
    }

    protected abstract void visitDir(FileCopyDetails dirDetails, CopySpec spec);

    protected abstract void visitFile(FileCopyDetails fileDetails, CopySpec spec);

    protected abstract void addDependency(Dependency dependency);

    protected abstract void addConflict(Dependency dependency);

    protected abstract void addObsolete(Dependency dependency);

    protected abstract void addDirectory(Directory directory);

    protected abstract void end() throws IOException;

    protected void startVisit() {
        tempDir = task.getTemporaryDir();
    }

    protected void endVisit() throws IOException {
        for (Dependency dependency : task.getResolvedDependencies().get()) {
            addDependency(dependency);
        }
        for (Dependency obsolete : task.getExten().getObsoletes().getOrElse(java.util.List.of())) {
            addObsolete(obsolete);
        }
        for (Dependency conflict : task.getExten().getConflicts().getOrElse(java.util.List.of())) {
            addConflict(conflict);
        }
        for (Directory directory : task.getExten().getDirectories()) {
            addDirectory(directory);
        }
        end();
    }

    /**
     * Concatenates script snippets into a single script, keeping a single shebang line at the top
     * and failing on conflicting shebang lines.
     */
    protected static String concat(Collection<String> scripts) {
        String shebang = null;
        StringBuilder result = new StringBuilder();
        for (String script : scripts) {
            if (script == null) {
                continue;
            }
            try (BufferedReader reader = new BufferedReader(new StringReader(script))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.matches("^#!.*$")) {
                        if (shebang == null) {
                            shebang = line;
                        } else if (line.equals(shebang) == false) {
                            throw new IllegalArgumentException("mismatching #! script lines");
                        }
                    } else {
                        result.append(line).append('\n');
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        if (shebang != null) {
            result.insert(0, shebang + "\n");
        }
        return result.toString();
    }

    /**
     * Resolves the copy spec a visited file was declared in. While decoupling the spec from the
     * stream is generally desirable, packaging needs the spec to read the packaging attributes
     * ({@link SpecAttributes}) and explicitly configured permissions. There is no public API for
     * this, hence the reflection.
     */
    protected static CopySpec extractSpec(FileCopyDetailsInternal fileDetails) {
        if (fileDetails instanceof DefaultFileCopyDetails == false) {
            return null;
        }
        try {
            Field specField = DefaultFileCopyDetails.class.getDeclaredField("specResolver");
            specField.setAccessible(true);
            CopySpecResolver specResolver = (CopySpecResolver) specField.get(fileDetails);

            Field specHolderField = DefaultCopySpec.DefaultCopySpecResolver.class.getDeclaredField("this$0");
            specHolderField.setAccessible(true);
            return (CopySpec) specHolderField.get(specResolver);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot extract copy spec from file details", e);
        }
    }

    /**
     * Provides the on-disk file for the visited details. Filtered files have no backing file and
     * are spooled to the task's temporary directory instead.
     */
    protected File extractFile(FileCopyDetails fileDetails) {
        try {
            return fileDetails.getFile();
        } catch (UnsupportedOperationException e) {
            File outputFile = new File(tempDir, fileDetails.getPath());
            fileDetails.copyTo(Objects.requireNonNull(outputFile));
            return outputFile;
        }
    }
}
