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

package org.elasticsearch.gradle.internal.ospackage.rpm;

import org.elasticsearch.gradle.internal.ospackage.Dependency;
import org.elasticsearch.gradle.internal.ospackage.PackageWriter;
import org.redline_rpm.Builder;
import org.redline_rpm.header.Architecture;
import org.redline_rpm.header.Header;
import org.redline_rpm.header.Os;
import org.redline_rpm.header.RpmType;
import org.redline_rpm.payload.Directive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Writes an rpm archive using the redline library.
 */
class RpmPackageWriter implements PackageWriter {

    private static final Logger logger = LoggerFactory.getLogger(RpmPackageWriter.class);

    private final Rpm task;
    private final Builder builder;

    RpmPackageWriter(Rpm task) {
        this.task = task;
        this.builder = new Builder();

        builder.setPackage(task.getPackageName().get(), task.getVersion().get(), task.getRelease().getOrElse(""), 0);
        builder.setType(RpmType.BINARY);
        Architecture architecture = task.getArchString() == null
            ? Architecture.NOARCH
            : Architecture.valueOf(task.getArchString().toUpperCase(Locale.ROOT));
        builder.setPlatform(architecture, task.getOs().getOrElse(Os.UNKNOWN));
        builder.setGroup(task.getPackageGroup().getOrNull());
        builder.setBuildHost(localHostName());
        builder.setSummary(task.getSummary().getOrElse(""));
        builder.setDescription(task.getPackageDescription().getOrElse(""));
        builder.setLicense(task.getLicense().getOrNull());
        builder.setPackager(task.getPackager().getOrNull());
        builder.setDistribution(task.getDistribution().getOrElse(""));
        builder.setVendor(task.getVendor().getOrElse(""));
        builder.setUrl(task.getUrl().getOrElse(""));
        List<String> prefixes = task.getPrefixes().getOrElse(List.of());
        if (prefixes.isEmpty() == false) {
            builder.setPrefixes(prefixes.toArray(new String[0]));
        }

        String signingKeyId = task.getSigningKeyId().getOrElse("");
        String signingKeyPassphrase = task.getSigningKeyPassphrase().getOrElse("");
        File signingKeyRingFile = task.getSigningKeyRingFile().isPresent() ? task.getSigningKeyRingFile().get().getAsFile() : null;
        if (signingKeyId.isBlank() == false
            && signingKeyPassphrase.isBlank() == false
            && signingKeyRingFile != null
            && signingKeyRingFile.exists()) {
            builder.setPrivateKeyId(signingKeyId);
            builder.setPrivateKeyPassphrase(signingKeyPassphrase);
            builder.setPrivateKeyRingFile(signingKeyRingFile);
        }

        // a source package is required, otherwise createrepo will assume the package is a source package
        String sourcePackage = task.getPackageName().get()
            + "-"
            + task.getVersion().get()
            + "-"
            + task.getRelease().getOrElse("")
            + "-src.rpm";
        builder.addHeaderEntry(Header.HeaderTag.SOURCERPM, sourcePackage);

        setScript(task.getPreInstallCommands().getOrElse(List.of()), builder::setPreInstallScript);
        setScript(task.getPostInstallCommands().getOrElse(List.of()), builder::setPostInstallScript);
        setScript(task.getPreUninstallCommands().getOrElse(List.of()), builder::setPreUninstallScript);
        setScript(task.getPostUninstallCommands().getOrElse(List.of()), builder::setPostUninstallScript);
        setScript(task.getPostTransCommands().getOrElse(List.of()), builder::setPostTransScript);

        for (Dependency dependency : task.getDependencies().getOrElse(List.of())) {
            builder.addDependency(dependency.packageName(), dependency.flag(), dependency.version());
        }
        for (Dependency obsolete : task.getObsoletes().getOrElse(List.of())) {
            builder.addObsoletes(obsolete.packageName(), obsolete.flag(), obsolete.version());
        }
        for (Dependency conflict : task.getConflicts().getOrElse(List.of())) {
            builder.addConflicts(conflict.packageName(), conflict.flag(), conflict.version());
        }
    }

    private void setScript(List<String> commands, ScriptSetter setter) {
        if (commands.isEmpty() == false) {
            setter.set(scriptWithDefines(commands));
        }
    }

    private interface ScriptSetter {
        void set(String script);
    }

    @Override
    public void addFile(String path, File source, int mode, String user, String group, int fileTypeFlags) throws IOException {
        logger.debug("adding file {}", path);
        Directive directive = fileTypeFlags == 0 ? null : new Directive(fileTypeFlags);
        try {
            builder.addFile(path, source, mode, -1, directive, user, group, false);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void addDirectory(String path, int mode, String user, String group) throws IOException {
        logger.debug("adding directory {}", path);
        try {
            builder.addDirectory(path, mode, null, user, group, false);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void addLink(String path, String target) throws IOException {
        logger.debug("adding link {} -> {}", path, target);
        try {
            builder.addLink(path, target);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void finish() throws IOException {
        File rpmFile = task.getArchiveFile().get().getAsFile();
        if (rpmFile.exists() && rpmFile.delete() == false) {
            throw new IOException("Cannot delete existing rpm file " + rpmFile);
        }
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(rpmFile, "rw")) {
            builder.build(randomAccessFile.getChannel());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        logger.info("created rpm {}", rpmFile);
    }

    private static String localHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "unknown";
        }
    }

    /**
     * Prepends the standard {@code RPM_*} environment defines to a maintainer script, mirroring
     * the behavior of rpmbuild, and normalizes the shebang line to the top.
     */
    private String scriptWithDefines(List<String> scripts) {
        List<String> parts = new ArrayList<>();
        parts.add(
            String.format(
                Locale.ROOT,
                " RPM_ARCH=%s \n RPM_OS=%s \n RPM_PACKAGE_NAME=%s \n RPM_PACKAGE_VERSION=%s \n RPM_PACKAGE_RELEASE=%s \n\n",
                task.getArchString(),
                task.getOs().isPresent() ? task.getOs().get().toString().toLowerCase(Locale.ROOT) : "",
                task.getPackageName().get(),
                task.getVersion().get(),
                task.getRelease().getOrElse("")
            )
        );
        parts.addAll(scripts);
        return concat(parts);
    }

    /**
     * Concatenates script snippets into a single script, keeping a single shebang line at the top
     * and failing on conflicting shebang lines.
     */
    private static String concat(List<String> scripts) {
        String shebang = null;
        StringBuilder result = new StringBuilder();
        for (String script : scripts) {
            if (script == null) {
                continue;
            }
            try (java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.StringReader(script))) {
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
                throw new java.io.UncheckedIOException(e);
            }
        }
        if (shebang != null) {
            result.insert(0, shebang + "\n");
        }
        return result.toString();
    }
}
