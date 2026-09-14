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

import groovy.text.GStringTemplateEngine;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;

/**
 * Renders the debian control file templates bundled as resources next to this class.
 */
class TemplateHelper {

    private final GStringTemplateEngine engine = new GStringTemplateEngine();
    private final File destDir;

    TemplateHelper(File destDir) {
        this.destDir = destDir;
    }

    File generateFile(String templateName, Map<String, Object> context) {
        try (InputStream stream = TemplateHelper.class.getResourceAsStream(templateName + ".ftl")) {
            if (stream == null) {
                throw new IllegalStateException("Missing template resource " + templateName + ".ftl");
            }
            try (BufferedReader template = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
                String content = engine.createTemplate(template).make(context).toString();
                File contentFile = new File(destDir, templateName);
                Files.createDirectories(destDir.toPath());
                Files.writeString(contentFile.toPath(), content, StandardCharsets.UTF_8);
                return contentFile;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Cannot compile template " + templateName, e);
        }
    }
}
