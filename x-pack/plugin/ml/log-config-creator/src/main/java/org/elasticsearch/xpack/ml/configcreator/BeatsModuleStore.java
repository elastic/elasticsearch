/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.grok.Grok;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.json.JsonXContent.jsonXContent;
import static org.elasticsearch.common.xcontent.yaml.YamlXContent.yamlXContent;

public final class BeatsModuleStore {

    private static final Set<String> BROAD_MATCH_FORMATS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        "apache2", "system"
    )));
    private static final Map<String, String> FORMAT_ABBREVIATIONS;
    static {
        Map<String, String> formatAbbreviations = new HashMap<>();
        formatAbbreviations.put("apache2", "apache");
        formatAbbreviations.put("mongodb", "mongo");
        formatAbbreviations.put("mysql", "my");
        formatAbbreviations.put("postgresql", "pg");
        FORMAT_ABBREVIATIONS = Collections.unmodifiableMap(formatAbbreviations);
    }

    private final List<BeatsModule> beatsModules;

    public BeatsModuleStore(Path moduleDir, String sampleFileName) throws IOException {
        beatsModules = (moduleDir != null && Files.isDirectory(moduleDir))
            ? Collections.unmodifiableList(populateModuleData(moduleDir, sampleFileName)) : Collections.emptyList();
    }

    public BeatsModule findMatchingModule(String message) {
        return beatsModules.stream().filter(beatsModule -> beatsModule.ingestPipelineMatchesMessage(message)).findFirst().orElse(null);
    }

    public BeatsModule findMatchingModule(Collection<String> messages) {
        if (messages.isEmpty()) {
            return null;
        }

        for (BeatsModule beatsModule : beatsModules) {
            if (messages.stream().allMatch(beatsModule::ingestPipelineMatchesMessage)) {
                return beatsModule;
            }
        }

        return null;
    }

    static List<BeatsModule> populateModuleData(Path moduleDir, String sampleFileName) throws IOException {

        List<BeatsModule> beatsModules = new ArrayList<>();

        try {
            Files.find(moduleDir, 3, (path, attrs) -> path.getFileName().toString().equals("manifest.yml"))
                .forEach(path -> considerModule(path, beatsModules, sampleFileName));
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }

        return beatsModules;
    }

    static void considerModule(Path manifestPath, List<BeatsModule> beatsModules, String sampleFileName) {
        String moduleName = manifestPath.getName(manifestPath.getNameCount() - 3).toString();

        // Ignore specialist modules that have no connection to the sample file name
        if (BROAD_MATCH_FORMATS.contains(moduleName) == false && sampleFileName.contains(moduleName) == false) {
            String abbreviation = FORMAT_ABBREVIATIONS.get(moduleName);
            if (abbreviation == null || sampleFileName.contains(abbreviation) == false) {
                return;
            }
        }

        parseModule(manifestPath, beatsModules, sampleFileName);
    }

    static void parseModule(Path manifestPath, List<BeatsModule> beatsModules, String sampleFileName) {
        int lastIndex = manifestPath.getNameCount() - 1;
        String moduleName = manifestPath.getName(lastIndex - 2).toString();
        String fileType = manifestPath.getName(lastIndex - 1).toString();
        try {
            try (XContentParser parser = yamlXContent.createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, Files.newInputStream(manifestPath))) {

                Map<String, Object> manifestContent = parser.map();
                if (manifestContent.containsKey("input") && manifestContent.containsKey("ingest_pipeline")) {
                    // The manifest should list a file containing the "inputs" section of the filebeat config, plus an ingest pipeline
                    // definition.  The files may contain variables to be substituted.  For the time being the only substitutions we make
                    // are:
                    // 1. "{{.format}}" in the ingest pipeline file name with "plain".
                    // 2. "{{$path}}" in the filebeat config with the path to the sample file that was provided.
                    // 3. "{< if .convert_timezone >}" is taken to be true.
                    // 4. "{{ _ingest.on_failure_message }}" is preserved.
                    // Other variables are simply removed.
                    String inputDefinition;
                    try (Stream<String> strm = Files.lines(manifestPath.getParent().resolve(manifestContent.get("input").toString()))) {
                        inputDefinition = strm.flatMap(line -> {
                            if (line.contains("{{$path}}")) {
                                return Stream.of("  " + line.replace("{{$path}}", "'" + sampleFileName + "'"));
                            } else if (line.contains("{{")) {
                                return Stream.empty();
                            } else if (line.startsWith("type: ")){
                                return Stream.of("- " + line);
                            } else {
                                return Stream.of("  " + line);
                            }
                        }).collect(Collectors.joining("\n"));
                    }
                    String ingestPipeline;
                    String ingestPipelineFileName = manifestContent.get("ingest_pipeline").toString().replace("{{.format}}", "plain");
                    try (Stream<String> strm = Files.lines(manifestPath.getParent().resolve(ingestPipelineFileName))) {
                        ingestPipeline = strm.flatMap(line -> {
                            if (line.contains("{< if .convert_timezone >}\"timezone\": \"{{ beat.timezone }}\",{< end >}")) {
                                return Stream.of(line.replace("{< if .convert_timezone >}\"timezone\": \"{{ beat.timezone }}\",{< end >}",
                                    "\"timezone\": \"{{ beat.timezone }}\","));
                            } else if (line.contains("{{") && line.contains("{{ _ingest.on_failure_message }}") == false) {
                                return Stream.empty();
                            } else {
                                return Stream.of(line);
                            }
                        }).collect(Collectors.joining("\n"));
                    }
                    if (inputDefinition.contains("- type: log\n") && ingestPipeline.contains("\"grok\"")) {
                        beatsModules.add(new BeatsModule(moduleName, fileType, inputDefinition, ingestPipeline));
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static class BeatsModule {

        public final String moduleName;
        public final String fileType;
        public final String inputDefinition;
        public final String ingestPipeline;
        private final List<Grok> groks;

        BeatsModule(String moduleName, String fileType, String inputDefinition, String ingestPipeline) throws IOException {
            this.moduleName = moduleName;
            this.fileType = fileType;
            this.inputDefinition = inputDefinition;
            this.ingestPipeline = ingestPipeline;
            groks = extractGrokPatternsFromIngestPipeline(ingestPipeline);
        }

        public boolean ingestPipelineMatchesMessage(String message) {
            return groks.stream().anyMatch(grok -> grok.match(message));
        }

        @SuppressWarnings("unchecked")
        private static List<Grok> extractGrokPatternsFromIngestPipeline(String ingestPipeline) throws IOException {

            try (XContentParser parser = jsonXContent.createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, ingestPipeline)) {

                List<Map<String, Object>> processors = (List<Map<String, Object>>) parser.map().get("processors");
                if (processors == null) {
                    return Collections.emptyList();
                }

                Map<String, Object> firstGrokProcessor = processors.stream().filter(processor -> processor.containsKey("grok")).findFirst()
                    .orElse(null);
                if (firstGrokProcessor == null) {
                    return Collections.emptyList();
                }

                List<String> patterns = (List<String>) ((Map<String, Object>) firstGrokProcessor.get("grok")).get("patterns");
                if (patterns == null) {
                    return Collections.emptyList();
                }

                Map<String, String> patternBank = new HashMap<>(Grok.getBuiltinPatterns());
                Map<String, String> customPatternDefinitions =
                    (Map<String, String>) ((Map<String, Object>) firstGrokProcessor.get("grok")).get("pattern_definitions");
                if (customPatternDefinitions != null) {
                    patternBank.putAll(customPatternDefinitions);
                }

                // Ignore patterns that are simply a %{GREEDYDATA:something}, as they will match too widely
                return patterns.stream()
                    .filter(pattern -> pattern.startsWith("%{GREEDYDATA") == false || pattern.indexOf('}') < pattern.length() - 1)
                    .map(pattern -> new Grok(patternBank, pattern)).collect(Collectors.toList());
            }
        }

        @Override
        public int hashCode() {
            // groks is NOT included as it should be equal if ingestPipeline is equal
            return Objects.hash(moduleName, fileType, inputDefinition, ingestPipeline);
        }

        @Override
        public boolean equals(Object other) {
            if (other == null) {
                return false;
            }
            if (getClass() != other.getClass()) {
                return false;
            }

            BeatsModule that = (BeatsModule) other;
            // groks is NOT included as it should be equal if ingestPipeline is equal
            return Objects.equals(this.moduleName, that.moduleName) &&
                Objects.equals(this.fileType, that.fileType) &&
                Objects.equals(this.inputDefinition, that.inputDefinition) &&
                Objects.equals(this.ingestPipeline, that.ingestPipeline);
        }

        @Override
        public String toString() {
            return "module name = '" + moduleName + "', file type = '" + fileType + "', input definition = '" + inputDefinition +
                "', ingest pipeline = '" + ingestPipeline + "'";
        }
    }
}
