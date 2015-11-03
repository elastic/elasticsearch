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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.env.Environment;
import org.elasticsearch.ingest.Data;
import org.elasticsearch.ingest.processor.Processor;

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
    private final String matchPattern;
    private final Grok grok;

    public GrokProcessor(Grok grok, String matchField, String matchPattern) throws IOException {
        this.matchField = matchField;
        this.matchPattern = matchPattern;
        this.grok = grok;
    }

    @Override
    public void execute(Data data) {
        Object field = data.getProperty(matchField);
        // TODO(talevy): handle invalid field types
        if (field instanceof String) {
            Map<String, Object> matches = grok.captures((String) field);
            if (matches != null) {
                matches.forEach((k, v) -> data.addField(k, v));
            }
        }
    }

    public static class Builder implements Processor.Builder {

        private Path grokConfigDirectory;
        private String matchField;
        private String matchPattern;

        public Builder(Path grokConfigDirectory) {
            this.grokConfigDirectory = grokConfigDirectory;
        }

        public void setMatchField(String matchField) {
            this.matchField = matchField;
        }

        public void setMatchPattern(String matchPattern) {
            this.matchPattern = matchPattern;
        }

        public void fromMap(Map<String, Object> config) {
            this.matchField = (String) config.get("field");
            this.matchPattern = (String) config.get("pattern");
        }

        @Override
        public Processor build() throws IOException {
            Map<String, String> patternBank = new HashMap<>();
            Path patternsDirectory = grokConfigDirectory.resolve("patterns");
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(patternsDirectory)) {
                for (Path patternFilePath : stream) {
                    try(InputStream is = Files.newInputStream(patternFilePath, StandardOpenOption.READ)) {
                        PatternUtils.loadBankFromStream(patternBank, is);
                    }
                }
            }

            Grok grok = new Grok(patternBank, matchPattern);
            return new GrokProcessor(grok, matchField, matchPattern);
        }

        public static class Factory implements Processor.Builder.Factory {
            private Path grokConfigDirectory;

            @Override
            public Processor.Builder create() {
                return new Builder(grokConfigDirectory);
            }

            @Override
            public void setConfigDirectory(Path configDirectory) {
                this.grokConfigDirectory = configDirectory.resolve("ingest").resolve("grok");
            }
        }

    }
}
