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

package org.elasticsearch.gradle.internal.ospackage.deb;

import org.elasticsearch.gradle.internal.ospackage.PackageWriter;
import org.gradle.api.GradleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vafer.jdeb.Compression;
import org.vafer.jdeb.Console;
import org.vafer.jdeb.DataProducer;
import org.vafer.jdeb.DebMaker;
import org.vafer.jdeb.producers.DataProducerLink;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Writes a deb archive using the jdeb library. The required control files and maintainer scripts
 * are materialized by {@link MaintainerScriptsGenerator} into a task-private debian directory.
 */
class DebPackageWriter implements PackageWriter {

    private static final Logger logger = LoggerFactory.getLogger(DebPackageWriter.class);

    private final Deb task;
    private final File debianDir;
    private final List<DataProducer> dataProducers = new ArrayList<>();

    DebPackageWriter(Deb task, File debianDir) throws IOException {
        this.task = task;
        this.debianDir = debianDir;
        if (Files.exists(debianDir.toPath())) {
            try (Stream<Path> paths = Files.walk(debianDir.toPath())) {
                paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                    try {
                        Files.delete(path);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            }
        }
        Files.createDirectories(debianDir.toPath());
    }

    @Override
    public void addFile(String path, File source, int mode, String user, String group, int fileTypeFlags) {
        logger.debug("adding file {}", path);
        dataProducers.add(new DataProducers.FileProducer(path, source, user, 0, group, 0, mode));
    }

    @Override
    public void addDirectory(String path, int mode, String user, String group) {
        logger.debug("adding directory {}", path);
        dataProducers.add(new DataProducers.DirProducer(path, user, 0, group, 0, mode));
    }

    @Override
    public void addLink(String path, String target) {
        logger.debug("adding link {} -> {}", path, target);
        dataProducers.add(new DataProducerLink(path, target, true, null, null, null));
    }

    @Override
    public void finish() throws IOException {
        new MaintainerScriptsGenerator(task, debianDir).generate();

        DebMaker maker = new DebMaker(new GradleLoggerConsole(), dataProducers, null);
        File debFile = task.getArchiveFile().get().getAsFile();
        maker.setControl(debianDir);
        maker.setDeb(debFile);

        String signingKeyId = task.getSigningKeyId().getOrElse("");
        String signingKeyPassphrase = task.getSigningKeyPassphrase().getOrElse("");
        File signingKeyRingFile = task.getSigningKeyRingFile().isPresent() ? task.getSigningKeyRingFile().get().getAsFile() : null;
        if (signingKeyId.isBlank() == false
            && signingKeyPassphrase.isBlank() == false
            && signingKeyRingFile != null
            && signingKeyRingFile.exists()) {
            maker.setKey(signingKeyId);
            maker.setPassphrase(signingKeyPassphrase);
            maker.setKeyring(signingKeyRingFile);
            maker.setSignPackage(true);
        }

        try {
            logger.info("creating debian package: {}", debFile);
            maker.setCompression(Compression.GZIP.toString());
            maker.makeDeb();
        } catch (Exception e) {
            throw new GradleException("Can't build debian package " + debFile, e);
        }
        logger.info("created deb {}", debFile);
    }

    private static class GradleLoggerConsole implements Console {
        @Override
        public void debug(String message) {
            logger.debug(message);
        }

        @Override
        public void info(String message) {
            logger.info(message);
        }

        @Override
        public void warn(String message) {
            logger.warn(message);
        }
    }

}
