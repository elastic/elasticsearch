/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.Terminal.Verbosity;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.grok.Grok;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.xcontent.json.JsonXContent.jsonXContent;
import static org.elasticsearch.common.xcontent.yaml.YamlXContent.yamlXContent;

/**
 * A store of Filebeat modules.
 */
public final class FilebeatModuleStore {

    private final List<FilebeatModule> filebeatModules;

    /**
     * Create an populate the store with modules that match the sample file name.
     * @param terminal A terminal to be used for informational messages.
     * @param modulePath Path to a Filebeat module directory.  This may be from either
     *                   the <code>beats</code> Git repo or from a Filebeat installation.
     * @param sampleFileName Name of the sample file provided by the user.  Only Filebeat
     *                       modules that could match the file name (assuming it was in
     *                       the correct directory) will be kept in the store.
     */
    public FilebeatModuleStore(Terminal terminal, Path modulePath, String sampleFileName) throws IOException {
        assert modulePath != null && Files.isDirectory(modulePath);
        terminal.println(Verbosity.VERBOSE, "Loading Filebeat modules from [" + modulePath + "]");
        filebeatModules = Collections.unmodifiableList(populateModuleData(modulePath, sampleFileName));
    }

    /**
     * Find a Filebeat module whose ingest pipeline would work on the message provided.
     * Since the store was originally populated with modules that match the sample file name,
     * this will also constrain what might be returned.
     * @param message The message that the returned Filebeat module must work with.
     * @return A Filebeat module that will work with the message provided, or <code>null</code> if none exists in the store.
     */
    public FilebeatModule findMatchingModule(String message) {
        return filebeatModules.stream()
            .filter(filebeatModule -> filebeatModule.ingestPipelineMatchesMessage(message)).findFirst().orElse(null);
    }

    /**
     * Find a Filebeat module whose ingest pipeline would work on all the messages in the
     * collection provided.  Since the store was originally populated with modules that
     * match the sample file name, this will also constrain what might be returned.
     * @param messages The messages that the returned Filebeat module must work with.
     * @return A Filebeat module that will work with all of the messages provided, or
     *         <code>null</code> if none exists in the store.  <code>null</code> is
     *         also returned if an empty collection of messages is provided.
     */
    public FilebeatModule findMatchingModule(Collection<String> messages) {
        if (messages.isEmpty()) {
            return null;
        }

        for (FilebeatModule filebeatModule : filebeatModules) {
            if (messages.stream().allMatch(filebeatModule::ingestPipelineMatchesMessage)) {
                return filebeatModule;
            }
        }

        return null;
    }

    static List<FilebeatModule> populateModuleData(Path modulePath, String sampleFileName) throws IOException {

        List<FilebeatModule> filebeatModules = new ArrayList<>();

        try {
            Files.find(modulePath, 3, (path, attrs) -> path.getFileName().toString().equals("manifest.yml"))
                .forEach(path -> parseModule(path, filebeatModules, sampleFileName));
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }

        return filebeatModules;
    }

    static void parseModule(Path manifestPath, List<FilebeatModule> filebeatModules, String sampleFileName) {
        int lastIndex = manifestPath.getNameCount() - 1;
        String moduleName = manifestPath.getName(lastIndex - 2).toString();
        String fileType = manifestPath.getName(lastIndex - 1).toString();
        try {
            try (XContentParser parser = yamlXContent.createParser(NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, Files.newInputStream(manifestPath))) {

                Map<String, Object> manifestContent = parser.map();
                if (manifestContent.containsKey("var") && manifestContent.containsKey("input") &&
                    manifestContent.containsKey("ingest_pipeline")) {
                    // The "var" section of the manifest will list the paths of files that it expects to work on by default.
                    // We'll only use it if the name of the sample file looks like a file from one of the default paths.
                    if (varsMatchSampleFileName(manifestContent.get("var"), sampleFileName)) {
                        // The manifest should also list a file containing the "inputs" section of the Filebeat config,
                        // plus a file containing an ingest pipeline definition.
                        String inputDefinition = parseInputDefinition(manifestPath, manifestContent.get("input"), sampleFileName);
                        String ingestPipeline = parseIngestPipeline(manifestPath, manifestContent.get("ingest_pipeline"));
                        if (inputDefinition.contains("- type: log\n") && ingestPipeline.contains("\"grok\"")) {
                            filebeatModules.add(new FilebeatModule(moduleName, fileType, inputDefinition, ingestPipeline));
                        }
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * This is an example of what we're trying to match against:
     *
     * var:
     *   - name: format
     *     default: plain
     *   - name: paths
     *     default:
     *       - /var/log/logstash/logstash-{{.format}}*.log
     *     os.windows:
     *       - c:/programdata/logstash/logs/logstash-{{.format}}*.log
     */
    @SuppressWarnings("unchecked")
    private static boolean varsMatchSampleFileName(Object varSection, String sampleFileName) {

        if (varSection instanceof List) {

            for (Object o : (List<Object>) varSection) {
                if (o instanceof Map) {
                    if (pathsMatchSampleFileNameWithoutPath((Map<String, Object>) o, sampleFileName.replaceFirst(".*[/\\\\]", ""))) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * This is an example of what we're trying to match against:
     *
     * name: paths
     * default:
     *   - /var/log/logstash/logstash-{{.format}}*.log
     * os.windows:
     *   - c:/programdata/logstash/logs/logstash-{{.format}}*.log
     */
    @SuppressWarnings("unchecked")
    private static boolean pathsMatchSampleFileNameWithoutPath(Map<String, Object> pathsSection, String sampleFileNameWithoutPath) {

        if ("paths".equals(pathsSection.get("name"))) {

            for (Map.Entry<String, Object> entry : pathsSection.entrySet()) {
                if (entry.getKey().equals("name")){
                    continue;
                }

                if (entry.getValue() instanceof List) {
                    List<Object> paths = (List<Object>) entry.getValue();
                    for (Object path : paths) {
                        if (pathMatchesSampleFileNameWithoutPath(path.toString(), sampleFileNameWithoutPath)) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * @param path All directories are stripped, to convert it to a file name.  Then variables are replaced with
     *             <code>*</code> so that they will match anything.  Finally it is converted from a file glob into
     *             a regex by replacing <code>*</code> and <code>?</code> with <code>.*</code> and <code>.</code>.
     * @param sampleFileNameWithoutPath The file to match against.  This must already have had any directories
     *                                  stripped such that it is a simple file name.
     * @return Does the regex created from {@code path} match any substring of {@code sampleFileNameWithoutPath}?
     */
    static boolean pathMatchesSampleFileNameWithoutPath(String path, String sampleFileNameWithoutPath) {
        String fileNamePattern = path.replaceFirst(".*[/\\\\]", "").replaceAll("\\{\\{[^}]*}}", "*").replace(".", "\\.")
            .replaceAll("\\*+", ".*").replace('?', '.');
        return Pattern.compile(fileNamePattern).matcher(sampleFileNameWithoutPath).find();
    }

    /**
     * Given a module manifest, parse the input definition, making adjustments so that it can be
     * incorporated into a complete filebeat config.
     * - "type" is prefixed with a dash to turn it into a list.
     * - "{{$path}}" in the Filebeat config is replaced with the path to the sample file that was provided.
     * - Other lines containing variables are removed.
     */
    private static String parseInputDefinition(Path manifestPath, Object inputSection, String sampleFileName) throws IOException {
        String inputDefinition;
        try (Stream<String> strm = Files.lines(manifestPath.getParent().resolve(inputSection.toString()))) {
            inputDefinition = strm.flatMap(line -> {
                if (line.contains("{{$path}}")) {
                    return Stream.of("  " + line.replace("{{$path}}", "'" + sampleFileName + "'"));
                } else if (line.contains("{{")) {
                    return Stream.empty();
                } else if (line.startsWith("type: ")){
                    return Stream.of("- " + line);
                } else if (line.equals("processors:")){
                    return Stream.of("\n" + line);
                } else {
                    return Stream.of("  " + line);
                }
            }).collect(Collectors.joining("\n"));
        }
        return inputDefinition;
    }

    /**
     * Given a module manifest, parse the ingest pipeline definition, making a few simplifying adjustments
     * - "{{.format}}" is replaced with "plain".
     * - "{&lt; if .convert_timezone &gt;}" is taken to be true.
     * - "{{ _ingest.on_failure_message }}" is preserved.
     * - Other lines containing variables are removed.
     */
    private static String parseIngestPipeline(Path manifestPath, Object ingestPipelineSection) throws IOException {
        String ingestPipeline;
        String ingestPipelineFileName = ingestPipelineSection.toString().replace("{{.format}}", "plain");
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
        return ingestPipeline;
    }

    public static class FilebeatModule {

        public final String moduleName;
        public final String fileType;
        public final String inputDefinition;
        public final String ingestPipeline;
        private final List<Grok> groks;

        FilebeatModule(String moduleName, String fileType, String inputDefinition, String ingestPipeline) throws IOException {
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

            FilebeatModule that = (FilebeatModule) other;
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
