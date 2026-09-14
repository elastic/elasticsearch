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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generates the debian maintainer scripts (preinst/postinst/prerm/postrm), the control file and
 * the conffiles listing from the bundled templates.
 */
class MaintainerScriptsGenerator {

    private final Deb task;
    private final TemplateHelper templateHelper;
    private final File destination;

    MaintainerScriptsGenerator(Deb task, TemplateHelper templateHelper, File destination) {
        this.task = task;
        this.templateHelper = templateHelper;
        this.destination = destination;
    }

    void generate(Map<String, Object> context) {
        templateHelper.generateFile("control", context);

        List<String> configurationFiles = task.getAllConfigurationFiles();
        if (configurationFiles.isEmpty() == false) {
            templateHelper.generateFile("conffiles", Map.of("files", configurationFiles));
        }

        record MaintainerScript(String name, File file, List<String> commands, boolean forceGeneration) {}
        List<MaintainerScript> scripts = List.of(
            new MaintainerScript("preinst", fileOrNull(task.getExten().getPreInstallFile()), task.getAllPreInstallCommands(), false),
            // postinst is also required when explicit install dirs need to be created
            new MaintainerScript(
                "postinst",
                fileOrNull(task.getExten().getPostInstallFile()),
                task.getAllPostInstallCommands(),
                hasDirs(context)
            ),
            new MaintainerScript("prerm", fileOrNull(task.getExten().getPreUninstallFile()), task.getAllPreUninstallCommands(), false),
            new MaintainerScript("postrm", fileOrNull(task.getExten().getPostUninstallFile()), task.getAllPostUninstallCommands(), false)
        );
        for (MaintainerScript script : scripts) {
            if (script.file() != null) {
                // a script file provided by the build is installed verbatim
                try {
                    Files.copy(script.file().toPath(), new File(destination, script.name()).toPath(), StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            } else if (script.commands().isEmpty() == false || script.forceGeneration()) {
                Map<String, Object> scriptContext = new HashMap<>(context);
                scriptContext.put("commands", script.commands().stream().map(MaintainerScriptsGenerator::stripShebang).toList());
                templateHelper.generateFile(script.name(), scriptContext);
            }
        }
    }

    private static File fileOrNull(org.gradle.api.file.RegularFileProperty property) {
        return property.isPresent() ? property.get().getAsFile() : null;
    }

    private static boolean hasDirs(Map<String, Object> context) {
        return context.get("dirs") instanceof List<?> dirs && dirs.isEmpty() == false;
    }

    static String installLine(DebCopyAction.InstallDir dir) {
        StringBuilder sb = new StringBuilder("install ");
        if (dir.user() != null && dir.user().isEmpty() == false) {
            sb.append("-o ").append(dir.user()).append(' ');
        }
        if (dir.group() != null && dir.group().isEmpty() == false) {
            sb.append("-g ").append(dir.group()).append(' ');
        }
        sb.append("-d ").append(dir.name());
        return sb.toString();
    }

    private static String stripShebang(String script) {
        StringBuilder result = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new StringReader(script))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.matches("^#!.*$") == false) {
                    result.append(line).append('\n');
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return result.toString();
    }
}
