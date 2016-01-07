/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.processor;

import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.ConfigurationUtils;
import org.elasticsearch.ingest.core.Processor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;

public final class GrokProcessor implements Processor {

    public static final String TYPE = "grok";

    private final String matchField;
    private final Grok grok;

    public GrokProcessor(Grok grok, String matchField) {
        this.matchField = matchField;
        this.grok = grok;
    }

    @Override
    public void execute(IngestDocument ingestDocument) throws Exception {
        String fieldValue = ingestDocument.getFieldValue(matchField, String.class);
        Map<String, Object> matches = grok.captures(fieldValue);
        if (matches != null) {
            matches.forEach((k, v) -> ingestDocument.setFieldValue(k, v));
        } else {
            throw new IllegalArgumentException("Grok expression does not match field value: [" + fieldValue + "]");
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String getMatchField() {
        return matchField;
    }

    Grok getGrok() {
        return grok;
    }

    public static class Factory implements Processor.Factory<GrokProcessor> {

        private final Path grokConfigDirectory;

        public Factory(Path configDirectory) {
            this.grokConfigDirectory = configDirectory.resolve("ingest").resolve("grok");
        }

        static void loadBankFromStream(Map<String, String> patternBank, InputStream inputStream) throws IOException {
            String line;
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            while ((line = br.readLine()) != null) {
                String trimmedLine = line.replaceAll("^\\s+", "");
                if (trimmedLine.startsWith("#") || trimmedLine.length() == 0) {
                    continue;
                }

                String[] parts = trimmedLine.split("\\s+", 2);
                if (parts.length == 2) {
                    patternBank.put(parts[0], parts[1]);
                }
            }
        }

        public GrokProcessor create(Map<String, Object> config) throws Exception {
            String matchField = ConfigurationUtils.readStringProperty(config, "field");
            String matchPattern = ConfigurationUtils.readStringProperty(config, "pattern");
            Map<String, String> customPatternBank = ConfigurationUtils.readOptionalMap(config, "pattern_definitions");

            Map<String, String> patternBank = new HashMap<>();

            Path patternsDirectory = grokConfigDirectory.resolve("patterns");
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(patternsDirectory)) {
                for (Path patternFilePath : stream) {
                    if (Files.isRegularFile(patternFilePath)) {
                        try(InputStream is = Files.newInputStream(patternFilePath, StandardOpenOption.READ)) {
                            loadBankFromStream(patternBank, is);
                        }
                    }
                }
            }

            if (customPatternBank != null) {
                patternBank.putAll(customPatternBank);
            }

            Grok grok = new Grok(patternBank, matchPattern);
            return new GrokProcessor(grok, matchField);
        }

    }

}
