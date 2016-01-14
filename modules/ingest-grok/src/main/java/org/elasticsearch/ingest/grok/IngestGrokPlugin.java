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

package org.elasticsearch.ingest.grok;

import org.elasticsearch.ingest.IngestModule;
import org.elasticsearch.plugins.Plugin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class IngestGrokPlugin extends Plugin {

    private static final String[] PATTERN_NAMES = new String[] {
        "aws", "bacula", "bro", "exim", "firewalls", "grok-patterns", "haproxy",
        "java", "junos", "linux-syslog", "mcollective-patterns", "mongodb", "nagios",
        "postgresql", "rails", "redis", "ruby"
    };

    private final Map<String, String> builtinPatterns;

    public IngestGrokPlugin() throws IOException {
        this.builtinPatterns = loadBuiltinPatterns();
    }

    @Override
    public String name() {
        return "ingest-grok";
    }

    @Override
    public String description() {
        return "Ingest processor that uses grok patterns to split text";
    }

    public void onModule(IngestModule ingestModule) {
        ingestModule.registerProcessor(GrokProcessor.TYPE, (environment, templateService) -> new GrokProcessor.Factory(builtinPatterns));
    }

    static Map<String, String> loadBuiltinPatterns() throws IOException {
        Map<String, String> builtinPatterns = new HashMap<>();
        for (String pattern : PATTERN_NAMES) {
            try(InputStream is = IngestGrokPlugin.class.getResourceAsStream("/patterns/" + pattern)) {
                loadPatterns(builtinPatterns, is);
            }
        }
        return Collections.unmodifiableMap(builtinPatterns);
    }

    private static void loadPatterns(Map<String, String> patternBank, InputStream inputStream) throws IOException {
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
}
