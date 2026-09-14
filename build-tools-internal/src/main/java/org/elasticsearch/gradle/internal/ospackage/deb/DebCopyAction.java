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

import org.elasticsearch.gradle.internal.ospackage.AbstractPackagingCopyAction;
import org.elasticsearch.gradle.internal.ospackage.Dependency;
import org.elasticsearch.gradle.internal.ospackage.Directory;
import org.elasticsearch.gradle.internal.ospackage.PackagingUtils;
import org.elasticsearch.gradle.internal.ospackage.SpecAttributes;
import org.gradle.api.GradleException;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.internal.file.copy.CopySpecInternal;
import org.gradle.api.internal.file.copy.FileCopyDetailsInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vafer.jdeb.Compression;
import org.vafer.jdeb.Console;
import org.vafer.jdeb.DataProducer;
import org.vafer.jdeb.DebMaker;
import org.vafer.jdeb.mapping.Mapper;
import org.vafer.jdeb.mapping.PermMapper;
import org.vafer.jdeb.producers.DataProducerPathTemplate;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Builds the deb file for a {@link Deb} task using the jdeb library.
 */
class DebCopyAction extends AbstractPackagingCopyAction<Deb> {

    private static final Logger logger = LoggerFactory.getLogger(DebCopyAction.class);
    private static final int SETGID_BIT = 02000;

    private final File debianDir;
    private final List<String> dependencies = new ArrayList<>();
    private final List<String> conflicts = new ArrayList<>();
    private final List<DataProducer> dataProducers = new ArrayList<>();
    private final List<InstallDir> installDirs = new ArrayList<>();
    private final DebFileVisitorStrategy debFileVisitorStrategy = new DebFileVisitorStrategy(dataProducers, installDirs);
    private final MaintainerScriptsGenerator maintainerScriptsGenerator;

    record InstallDir(String name, String user, String group) {}

    DebCopyAction(Deb task, File debianDir) {
        super(task);
        validate(task);
        this.debianDir = debianDir;
        this.maintainerScriptsGenerator = new MaintainerScriptsGenerator(task, new TemplateHelper(debianDir), debianDir);
    }

    private static void validate(Deb task) {
        String version = task.getVersion();
        if (version == null
            || version.isEmpty()
            || Character.isDigit(version.charAt(0)) == false
            || version.matches("[A-Za-z0-9.+:~-]+") == false) {
            throw new InvalidUserDataException(
                "Invalid upstream version '" + version + "' - a valid version must start with a digit and only contain [A-Za-z0-9.+:~-]"
            );
        }
        String packageName = task.getPackageName();
        if (packageName == null
            || packageName.length() < 2
            || Character.isLetterOrDigit(packageName.charAt(0)) == false
            || packageName.matches("[a-z0-9.+-]+") == false) {
            throw new InvalidUserDataException(
                "Invalid package name '"
                    + packageName
                    + "' - a valid package name must start with an alphanumeric character, have a length of at least two"
                    + " characters and only contain [a-z0-9.+-]"
            );
        }
    }

    @Override
    protected void startVisit() {
        super.startVisit();
        try {
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
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected void visitFile(FileCopyDetailsInternal fileDetails, CopySpecInternal spec) {
        logger.debug("adding file {}", fileDetails.getRelativePath().getPathString());

        File inputFile = extractFile(fileDetails);

        String user = lookupOrDefault(spec, SpecAttributes.USER, task.getUser());
        String group = lookupOrDefault(spec, SpecAttributes.PERMISSION_GROUP, task.getPermissionGroup());
        int uid = task.getExten().getUid().getOrElse(0);
        int gid = task.getExten().getGid().getOrElse(0);

        Integer explicitMode = PackagingUtils.getFileMode(spec);
        int fileMode = explicitMode != null ? explicitMode : PackagingUtils.getUnixPermission(fileDetails);

        debFileVisitorStrategy.addFile(fileDetails, inputFile, user, uid, group, gid, fileMode);
    }

    @Override
    protected void visitDir(FileCopyDetailsInternal dirDetails, CopySpecInternal spec) {
        boolean createDirectoryEntry = lookupOrDefault(
            spec,
            SpecAttributes.CREATE_DIRECTORY_ENTRY,
            task.getExten().getCreateDirectoryEntry().get()
        );
        if (createDirectoryEntry == false) {
            return;
        }
        logger.debug("adding directory {}", dirDetails.getRelativePath().getPathString());

        String user = lookupOrDefault(spec, SpecAttributes.USER, task.getUser());
        String group = lookupOrDefault(spec, SpecAttributes.PERMISSION_GROUP, task.getPermissionGroup());
        int uid = task.getExten().getUid().getOrElse(0);
        int gid = task.getExten().getGid().getOrElse(0);

        Integer explicitDirMode = PackagingUtils.getDirMode(spec);
        int dirMode = explicitDirMode != null ? explicitDirMode : PackagingUtils.getUnixPermission(dirDetails);
        boolean setgid = lookupOrDefault(spec, SpecAttributes.SETGID, task.getExten().getSetgid().get());
        if (setgid) {
            dirMode = dirMode | SETGID_BIT;
        }
        debFileVisitorStrategy.addDirectory(dirDetails, user, uid, group, gid, dirMode);
    }

    @SuppressWarnings("unchecked")
    private static <T> T lookupOrDefault(CopySpecInternal spec, String key, T taskDefault) {
        Object value = SpecAttributes.lookup(spec, key);
        return value != null ? (T) value : taskDefault;
    }

    @Override
    protected void addDependency(Dependency dependency) {
        dependencies.add(dependency.toDebString());
    }

    @Override
    protected void addConflict(Dependency dependency) {
        conflicts.add(dependency.toDebString());
    }

    @Override
    protected void addObsolete(Dependency dependency) {
        logger.warn("obsoletes functionality not implemented for deb files");
    }

    @Override
    protected void addDirectory(Directory directory) {
        dataProducers.add(
            new DataProducerPathTemplate(
                new String[] { directory.getPath() },
                null,
                null,
                new Mapper[] { new PermMapper(-1, -1, task.getUser(), task.getPermissionGroup(), directory.getPermissions(), -1, 0, null) }
            )
        );
    }

    @Override
    protected void end() {
        maintainerScriptsGenerator.generate(toContext());

        DebMaker maker = new DebMaker(new GradleLoggerConsole(), dataProducers, null);
        File debFile = task.getArchiveFile().get().getAsFile();
        maker.setControl(debianDir);
        maker.setDeb(debFile);

        String signingKeyId = task.getExten().getSigningKeyId().getOrElse("");
        String signingKeyPassphrase = task.getExten().getSigningKeyPassphrase().getOrElse("");
        File signingKeyRingFile = task.getExten().getSigningKeyRingFile().isPresent()
            ? task.getExten().getSigningKeyRingFile().get().getAsFile()
            : null;
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

    /**
     * Assembles the context consumed by the debian control and maintainer script templates.
     */
    private Map<String, Object> toContext() {
        Map<String, Object> context = new LinkedHashMap<>();
        context.put("name", task.getPackageName());
        context.put("version", task.getArchiveVersion().get());
        context.put("release", task.getRelease());
        context.put("maintainer", task.getMaintainer());
        context.put("uploaders", task.getExten().getUploaders().getOrElse(""));
        context.put("priority", task.getExten().getPriority().getOrElse(""));
        context.put("epoch", task.getExten().getEpoch().getOrElse(0));
        context.put("description", task.getExten().getPackageDescription().getOrElse(""));
        context.put("distribution", task.getDistribution());
        context.put("summary", task.getExten().getSummary().getOrElse(""));
        context.put("section", task.getPackageGroup());
        context.put("time", new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss Z", Locale.ROOT).format(new Date()));
        context.put("provides", "");
        context.put("depends", String.join(", ", dependencies));
        context.put("url", task.getExten().getUrl().getOrElse(""));
        context.put("arch", task.getArchString());
        context.put("multiArch", "");
        context.put("conflicts", String.join(", ", conflicts));
        context.put("recommends", "");
        context.put("suggests", "");
        context.put("enhances", "");
        context.put("preDepends", "");
        context.put("breaks", "");
        context.put("replaces", "");
        context.put("fullVersion", buildFullVersion());
        // in the deb control file, header XB-Foo becomes Foo in the binary package
        Map<String, String> customFields = new LinkedHashMap<>();
        task.getAllCustomFields().forEach((key, value) -> customFields.put("XB-" + capitalize(key), value));
        context.put("customFields", customFields);
        context.put("dirs", installDirs.stream().map(dir -> Map.of("install", MaintainerScriptsGenerator.installLine(dir))).toList());
        return context;
    }

    private static String capitalize(String value) {
        return value.isEmpty() ? value : Character.toUpperCase(value.charAt(0)) + value.substring(1);
    }

    private String buildFullVersion() {
        StringBuilder fullVersion = new StringBuilder();
        int epoch = task.getExten().getEpoch().getOrElse(0);
        if (epoch != 0) {
            fullVersion.append(epoch).append(':');
        }
        fullVersion.append(task.getVersion());
        if (task.getRelease() != null && task.getRelease().isEmpty() == false) {
            fullVersion.append('-').append(task.getRelease());
        }
        return fullVersion.toString();
    }
}
