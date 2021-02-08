/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.rest.compat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import org.elasticsearch.gradle.test.rest.transform.headers.InjectHeaders;
import org.elasticsearch.gradle.test.rest.transform.match.AddMatch;
import org.elasticsearch.gradle.test.rest.transform.match.RemoveMatch;
import org.elasticsearch.gradle.test.rest.transform.match.ReplaceMatch;
import org.elasticsearch.gradle.test.rest.transform.RestTestTransform;
import org.elasticsearch.gradle.test.rest.transform.RestTestTransformer;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.util.PatternFilterable;
import org.gradle.api.tasks.util.PatternSet;
import org.gradle.internal.Factory;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.gradle.internal.rest.compat.YamlRestCompatTestPlugin.COMPATIBLE_VERSION;

public class RestCompatTestTransformTask extends DefaultTask {

    private static final YAMLFactory YAML_FACTORY = new YAMLFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(YAML_FACTORY);
    private static final ObjectReader READER = MAPPER.readerFor(ObjectNode.class);
    private static final ObjectWriter WRITER = MAPPER.writerFor(ObjectNode.class);

    private static final Map<String, String> headers = Map.of(
        "Content-Type",
        "application/vnd.elasticsearch+json;compatible-with=" + COMPATIBLE_VERSION,
        "Accept",
        "application/vnd.elasticsearch+json;compatible-with=" + COMPATIBLE_VERSION
    );

    private FileCollection input;
    private File output;
    private static final String REST_TEST_PREFIX = "rest-api-spec/test";

    private final PatternFilterable testPatternSet;
    private final List<RestTestTransform<?>> transformations = new ArrayList<>();
    private final Map<String, List<RestTestTransform<?>>> perTestTransformation = new HashMap<>();


    @Inject
    public RestCompatTestTransformTask(Factory<PatternSet> patternSetFactory) {
        this.testPatternSet = patternSetFactory.create();
        this.testPatternSet.include("/*" + "*/*.yml"); // concat these strings to keep build from thinking this is invalid javadoc
        // always inject compat headers
        transformations.add(new InjectHeaders(headers));
    }


    public void replaceMatch(String subKey, Object value) {
        transformations.add(new ReplaceMatch(subKey, MAPPER.convertValue(value, JsonNode.class)));
    }

    public void replaceMatch(String subKey, Object value, String testName) {
        transformations.add(new ReplaceMatch(subKey, MAPPER.convertValue(value, JsonNode.class), testName));
    }

    public void removeMatch(String subKey) {
        transformations.add(new RemoveMatch(subKey));
    }

    public void removeMatch(String subKey, String testName) {
        transformations.add(new RemoveMatch(subKey, testName));
    }

    public void addMatch(String subKey, Object value, String testName) {
        transformations.add(new AddMatch(subKey, MAPPER.convertValue(value, JsonNode.class), testName));
    }

    @OutputDirectory
    public File getOutputDir() {
        return output;
    }

    @SkipWhenEmpty
    @InputFiles
    public FileTree getTestFiles() {
        return input.getAsFileTree().matching(testPatternSet);
    }

    @TaskAction
    public void transform() throws IOException {
        RestTestTransformer transformer = new RestTestTransformer();
        for (File file : getTestFiles().getFiles()) {
            YAMLParser yamlParser = YAML_FACTORY.createParser(file);
            List<ObjectNode> tests = READER.<ObjectNode>readValues(yamlParser).readAll();
            List<ObjectNode> transformRestTests = transformer.transformRestTests(new LinkedList<>(tests), transformations);
            // convert to url to ensure forward slashes
            String[] testFileParts = file.toURI().toURL().getPath().split(REST_TEST_PREFIX);
            if (testFileParts.length != 2) {
                throw new IllegalArgumentException("could not split " + file + " into expected parts");
            }
            File output = new File(getOutputDir(), testFileParts[1]);
            output.getParentFile().mkdirs();
            try (SequenceWriter sequenceWriter = WRITER.writeValues(output)) {
                for (ObjectNode transformedTest : transformRestTests) {
                    sequenceWriter.write(transformedTest);
                }
            }
        }
    }

    public void setInput(FileCollection input) {
        this.input = input;
    }

    public void setOutput(File output) {
        this.output = output;
    }
}
