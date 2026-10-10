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

import org.elasticsearch.gradle.internal.ospackage.Dependency;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Generates the debian control metadata files required by the Elasticsearch package build and
 * materializes maintainer scripts in the same form as the main-branch package build.
 */
class MaintainerScriptsGenerator {

    private static final String CONTROL_FILE = "control";
    private static final String CONFFILES_FILE = "conffiles";

    private final Deb task;
    private final File destination;

    MaintainerScriptsGenerator(Deb task, File destination) {
        this.task = task;
        this.destination = destination;
    }

    void generate() {
        writeFile(CONTROL_FILE, buildControlFile());

        List<String> configurationFiles = task.getConfigurationFiles().getOrElse(List.of());
        if (configurationFiles.isEmpty() == false) {
            writeFile(CONFFILES_FILE, String.join("\n", configurationFiles) + "\n\n");
        }

        writeScript("preinst", "#!/bin/bash -e\n", 1, task.getPreInstallCommands().getOrElse(List.of()));
        writeScript("postinst", "#!/bin/bash -e\n", 1, task.getPostInstallCommands().getOrElse(List.of()));
        writeScript("prerm", "#!/bin/sh -e\n\n\n", 2, task.getPreUninstallCommands().getOrElse(List.of()));
        writeScript("postrm", "#!/bin/sh -e\n\n\n", 2, task.getPostUninstallCommands().getOrElse(List.of()));
    }

    private String buildControlFile() {
        StringBuilder content = new StringBuilder();
        appendField(content, "Source", task.getPackageName().get());
        appendField(content, "Section", task.getPackageGroup().getOrNull());
        appendField(content, "Priority", "optional");
        appendField(content, "Maintainer", task.getMaintainer().getOrElse(""));
        appendField(content, "Uploaders", "");
        appendField(content, "Version", buildFullVersion());
        appendField(content, "Standards-Version", "3.8.3");
        appendField(content, "Package", task.getPackageName().get());
        appendOptionalField(content, "Homepage", task.getUrl().getOrElse(""));
        appendField(content, "Architecture", task.getArchString());
        appendField(content, "Distribution", task.getDistribution().getOrElse(""));
        appendField(content, "Depends", joinDependencies(task.getDependencies().getOrElse(List.of())));

        String conflicts = joinDependencies(task.getConflicts().getOrElse(List.of()));
        appendOptionalField(content, "Conflicts", conflicts);

        for (Map.Entry<String, String> customField : customFields().entrySet()) {
            appendField(content, customField.getKey(), customField.getValue());
        }

        appendField(content, "Description", task.getSummary().getOrElse(""));
        for (String line : task.getPackageDescription().getOrElse("").split("\\R", -1)) {
            content.append(' ').append(line).append('\n');
        }
        return content.toString();
    }

    private Map<String, String> customFields() {
        Map<String, String> customFields = new LinkedHashMap<>();
        task.getCustomFields().getOrElse(Map.of()).forEach((key, value) -> customFields.put("XB-" + capitalize(key), value));
        return customFields;
    }

    private void writeScript(String name, String shebang, int trailingNewlines, List<String> commands) {
        if (commands.isEmpty()) {
            return;
        }
        StringBuilder content = new StringBuilder(shebang).append(stripShebangs(commands));
        for (int i = 0; i < trailingNewlines; i++) {
            content.append('\n');
        }
        writeFile(name, content.toString());
    }

    private static String stripShebangs(List<String> scripts) {
        StringBuilder result = new StringBuilder();
        for (String script : scripts) {
            if (script == null) {
                continue;
            }
            try (BufferedReader reader = new BufferedReader(new StringReader(script))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("#!") == false) {
                        result.append(line).append('\n');
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return result.toString();
    }

    private static void appendField(StringBuilder content, String key, String value) {
        content.append(key).append(": ").append(value == null ? "" : value).append('\n');
    }

    private static void appendOptionalField(StringBuilder content, String key, String value) {
        if (value != null && value.isEmpty() == false) {
            appendField(content, key, value);
        }
    }

    private static String joinDependencies(List<Dependency> dependencies) {
        return String.join(", ", dependencies.stream().map(Dependency::toDebString).toList());
    }

    private static String capitalize(String value) {
        return value.isEmpty() ? value : Character.toUpperCase(value.charAt(0)) + value.substring(1);
    }

    private String buildFullVersion() {
        StringBuilder fullVersion = new StringBuilder(task.getVersion().get());
        String release = task.getRelease().getOrNull();
        if (release != null && release.isEmpty() == false) {
            fullVersion.append('-').append(release);
        }
        return fullVersion.toString();
    }

    private void writeFile(String name, String content) {
        try {
            Files.createDirectories(destination.toPath());
            Files.writeString(new File(destination, name).toPath(), content, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
