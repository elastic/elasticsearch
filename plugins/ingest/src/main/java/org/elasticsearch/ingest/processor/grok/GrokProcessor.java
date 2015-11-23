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

package org.elasticsearch.ingest.processor.grok;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.processor.ConfigurationUtils;
import org.elasticsearch.ingest.processor.Processor;

import java.io.IOException;
import java.io.InputStream;
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

    public GrokProcessor(Grok grok, String matchField) throws IOException {
        this.matchField = matchField;
        this.grok = grok;
    }

    @Override
    public void execute(IngestDocument ingestDocument) {
        Object field = ingestDocument.getPropertyValue(matchField, Object.class);
        // TODO(talevy): handle invalid field types
        if (field instanceof String) {
            Map<String, Object> matches = grok.captures((String) field);
            if (matches != null) {
                matches.forEach((k, v) -> ingestDocument.setPropertyValue(k, v));
            }
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
        private Path grokConfigDirectory;

        public GrokProcessor create(Map<String, Object> config) throws IOException {
            String matchField = ConfigurationUtils.readStringProperty(config, "field");
            String matchPattern = ConfigurationUtils.readStringProperty(config, "pattern");
            Map<String, String> patternBank = new HashMap<>();
            Path patternsDirectory = grokConfigDirectory.resolve("patterns");
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(patternsDirectory)) {
                for (Path patternFilePath : stream) {
                    if (Files.isRegularFile(patternFilePath)) {
                        try(InputStream is = Files.newInputStream(patternFilePath, StandardOpenOption.READ)) {
                            PatternUtils.loadBankFromStream(patternBank, is);
                        }
                    }
                }
            }

            Grok grok = new Grok(patternBank, matchPattern);
            return new GrokProcessor(grok, matchField);
        }

        @Override
        public void setConfigDirectory(Path configDirectory) {
            this.grokConfigDirectory = configDirectory.resolve("ingest").resolve("grok");
        }
    }

}
