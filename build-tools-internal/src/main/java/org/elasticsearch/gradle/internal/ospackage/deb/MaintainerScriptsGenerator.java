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

import org.gradle.api.file.RegularFileProperty;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Generates the debian control file and the conffiles listing from the bundled templates and
 * installs the maintainer scripts (preinst/postinst/prerm/postrm). Script files declared on the
 * task are installed verbatim; the templated postinst is only used when explicit install dirs
 * require a generated script.
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

        List<String> configurationFiles = task.getConfigurationFiles().getOrElse(List.of());
        if (configurationFiles.isEmpty() == false) {
            templateHelper.generateFile("conffiles", Map.of("files", configurationFiles));
        }

        record MaintainerScript(String name, File file, boolean forceGeneration) {}
        List<MaintainerScript> scripts = List.of(
            new MaintainerScript("preinst", fileOrNull(task.getPreInstallFile()), false),
            // postinst is also required when explicit install dirs need to be created
            new MaintainerScript("postinst", fileOrNull(task.getPostInstallFile()), hasDirs(context)),
            new MaintainerScript("prerm", fileOrNull(task.getPreUninstallFile()), false),
            new MaintainerScript("postrm", fileOrNull(task.getPostUninstallFile()), false)
        );
        for (MaintainerScript script : scripts) {
            if (script.file() != null) {
                installScript(script.file(), new File(destination, script.name()));
            } else if (script.forceGeneration()) {
                Map<String, Object> scriptContext = new HashMap<>(context);
                scriptContext.put("commands", List.of());
                templateHelper.generateFile(script.name(), scriptContext);
            }
        }
    }

    private static File fileOrNull(RegularFileProperty property) {
        return property.isPresent() ? property.get().getAsFile() : null;
    }

    /**
     * Nebula/jdeb historically wrapped these maintainer scripts so they executed under bash even
     * when the source file itself lacked a shebang. Preserve that behavior here because the shared
     * Elasticsearch postinst script uses bash syntax (`<<<`).
     */
    private static void installScript(File source, File target) {
        try {
            String content = Files.readString(source.toPath(), StandardCharsets.UTF_8);
            if (content.startsWith("#!") == false) {
                content = "#!/bin/bash\n" + content;
            }
            Files.writeString(target.toPath(), content, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static boolean hasDirs(Map<String, Object> context) {
        return context.get("dirs") instanceof List<?> dirs && dirs.isEmpty() == false;
    }

    static String installLine(DebPackageWriter.InstallDir dir) {
        StringBuilder sb = new StringBuilder("install ");
        if (dir.user() != null && dir.user().isEmpty() == false) {
            sb.append("-o ").append(dir.user()).append(' ');
        }
        if (dir.group() != null && dir.group().isEmpty() == false) {
            sb.append("-g ").append(dir.group()).append(' ');
        }
        sb.append("-m ").append(String.format(Locale.ROOT, "%04o", dir.mode() & 07777)).append(' ');
        sb.append("-d ").append(dir.name());
        return sb.toString();
    }
}
