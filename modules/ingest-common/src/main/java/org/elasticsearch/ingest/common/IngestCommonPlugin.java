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

package org.elasticsearch.ingest.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;

public class IngestCommonPlugin extends Plugin implements IngestPlugin {

    private final Map<String, String> builtinPatterns;

    public IngestCommonPlugin() throws IOException {
        this.builtinPatterns = loadBuiltinPatterns();
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        Map<String, Processor.Factory> processors = new HashMap<>();
        processors.put(DateProcessor.TYPE, new DateProcessor.Factory());
        processors.put(SetProcessor.TYPE, new SetProcessor.Factory(parameters.templateService));
        processors.put(AppendProcessor.TYPE, new AppendProcessor.Factory(parameters.templateService));
        processors.put(RenameProcessor.TYPE, new RenameProcessor.Factory());
        processors.put(RemoveProcessor.TYPE, new RemoveProcessor.Factory(parameters.templateService));
        processors.put(SplitProcessor.TYPE, new SplitProcessor.Factory());
        processors.put(JoinProcessor.TYPE, new JoinProcessor.Factory());
        processors.put(UppercaseProcessor.TYPE, new UppercaseProcessor.Factory());
        processors.put(LowercaseProcessor.TYPE, new LowercaseProcessor.Factory());
        processors.put(TrimProcessor.TYPE, new TrimProcessor.Factory());
        processors.put(ConvertProcessor.TYPE, new ConvertProcessor.Factory());
        processors.put(GsubProcessor.TYPE, new GsubProcessor.Factory());
        processors.put(FailProcessor.TYPE, new FailProcessor.Factory(parameters.templateService));
        processors.put(ForEachProcessor.TYPE, new ForEachProcessor.Factory());
        processors.put(DateIndexNameProcessor.TYPE, new DateIndexNameProcessor.Factory());
        processors.put(SortProcessor.TYPE, new SortProcessor.Factory());
        processors.put(GrokProcessor.TYPE, new GrokProcessor.Factory(builtinPatterns));
        processors.put(ScriptProcessor.TYPE, new ScriptProcessor.Factory(parameters.scriptService));
        processors.put(DotExpanderProcessor.TYPE, new DotExpanderProcessor.Factory());
        processors.put(JsonProcessor.TYPE, new JsonProcessor.Factory());
        processors.put(KeyValueProcessor.TYPE, new KeyValueProcessor.Factory());
        return Collections.unmodifiableMap(processors);
    }

    // Code for loading built-in grok patterns packaged with the jar file:

    private static final String[] PATTERN_NAMES = new String[] {
            "aws", "bacula", "bro", "exim", "firewalls", "grok-patterns", "haproxy",
            "java", "junos", "linux-syslog", "mcollective-patterns", "mongodb", "nagios",
            "postgresql", "rails", "redis", "ruby"
    };

    public static Map<String, String> loadBuiltinPatterns() throws IOException {
        Map<String, String> builtinPatterns = new HashMap<>();
        for (String pattern : PATTERN_NAMES) {
            try(InputStream is = IngestCommonPlugin.class.getResourceAsStream("/patterns/" + pattern)) {
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
