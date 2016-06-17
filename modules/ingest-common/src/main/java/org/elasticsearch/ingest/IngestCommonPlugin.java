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

package org.elasticsearch.ingest;

import org.elasticsearch.node.NodeModule;
import org.elasticsearch.plugins.Plugin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class IngestCommonPlugin extends Plugin {

    public static final String NAME = "ingest-common";

    private final Map<String, String> builtinPatterns;

    public IngestCommonPlugin() throws IOException {
        this.builtinPatterns = loadBuiltinPatterns();
    }

    public void onModule(NodeModule nodeModule) {
        nodeModule.registerProcessor(DateProcessor.TYPE, (registry) -> new DateProcessor.Factory());
        nodeModule.registerProcessor(SetProcessor.TYPE, (registry) -> new SetProcessor.Factory(registry.getTemplateService()));
        nodeModule.registerProcessor(AppendProcessor.TYPE, (registry) -> new AppendProcessor.Factory(registry.getTemplateService()));
        nodeModule.registerProcessor(RenameProcessor.TYPE, (registry) -> new RenameProcessor.Factory());
        nodeModule.registerProcessor(RemoveProcessor.TYPE, (registry) -> new RemoveProcessor.Factory(registry.getTemplateService()));
        nodeModule.registerProcessor(SplitProcessor.TYPE, (registry) -> new SplitProcessor.Factory());
        nodeModule.registerProcessor(JoinProcessor.TYPE, (registry) -> new JoinProcessor.Factory());
        nodeModule.registerProcessor(UppercaseProcessor.TYPE, (registry) -> new UppercaseProcessor.Factory());
        nodeModule.registerProcessor(LowercaseProcessor.TYPE, (registry) -> new LowercaseProcessor.Factory());
        nodeModule.registerProcessor(TrimProcessor.TYPE, (registry) -> new TrimProcessor.Factory());
        nodeModule.registerProcessor(ConvertProcessor.TYPE, (registry) -> new ConvertProcessor.Factory());
        nodeModule.registerProcessor(GsubProcessor.TYPE, (registry) -> new GsubProcessor.Factory());
        nodeModule.registerProcessor(FailProcessor.TYPE, (registry) -> new FailProcessor.Factory(registry.getTemplateService()));
        nodeModule.registerProcessor(ForEachProcessor.TYPE, (registry) -> new ForEachProcessor.Factory(registry));
        nodeModule.registerProcessor(DateIndexNameProcessor.TYPE, (registry) -> new DateIndexNameProcessor.Factory());
        nodeModule.registerProcessor(SortProcessor.TYPE, (registry) -> new SortProcessor.Factory());
        nodeModule.registerProcessor(GrokProcessor.TYPE, (registry) -> new GrokProcessor.Factory(builtinPatterns));
        nodeModule.registerProcessor(ScriptProcessor.TYPE, (registry) ->
            new ScriptProcessor.Factory(registry.getScriptService(), registry.getClusterService()));
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
