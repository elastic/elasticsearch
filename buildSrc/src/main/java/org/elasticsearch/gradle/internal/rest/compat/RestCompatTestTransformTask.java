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
import org.elasticsearch.gradle.test.rest.transform.match.ReplaceMatch;
import org.elasticsearch.gradle.test.rest.transform.RestTestTransform;
import org.elasticsearch.gradle.test.rest.transform.RestTestTransformer;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.tasks.Input;
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

    private static JsonNodeFactory jsonNodeFactory = JsonNodeFactory.withExactBigDecimals(false);

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
    //Use a string to represent the transformations...this is simpler then ensuring correct serialization of objects
    private StringBuilder transformationsKey = new StringBuilder();


    @Inject
    public RestCompatTestTransformTask(Factory<PatternSet> patternSetFactory) {
        this.testPatternSet = patternSetFactory.create();
        this.testPatternSet.include("/*" + "*/*.yml"); // concat these strings to keep build from thinking this is invalid javadoc
        // always inject compat headers
        transformations.add(new InjectHeaders(headers));
    }

//    /**
//     * Replaces all of the REST Test match assertion for the given project. For example:
//     * <p>
//     * given: <br/>
//     * "match":{"_type": "foo"} <br/>
//     * but want to transform to: <br/>
//     * "match":{"_type": "_doc"} <br/>
//     * <br/>
//     * You can use this method with "_type" as the subKey, and "_doc" as the value.
//     * All compatible REST tests for this project will be updated.
//     * </p>
//     * {@link ObjectMapper#convertValue(java.lang.Object, java.lang.Class)} is used to convert the value into to the appropriate JSON type.
//     *
//     * @param subKey The direct child key of the "match" object to replace
//     * @param value  The value use to replace the existing value
//     */
    public void replaceAllMatch(String subKey, Object value) {
        ObjectNode replacementNode = new ObjectNode(jsonNodeFactory);
        replacementNode.set(subKey, MAPPER.convertValue(value, JsonNode.class));
        transformations.add(new ReplaceMatch(subKey, replacementNode, null));
        transformationsKey.append("replace_match_all").append("subKey").append(replacementNode.toString());
    }

//    /**
//     * Replaces a REST Test match assertion for a specific test in the project. For example:
//     * <p>
//     * given: <br/>
//     * "match":{"items.0.index.status":400} <br/>
//     * but want to transform to: <br/>
//     * "match":{"items.0.index.status":200} <br/>
//     * <br/>
//     * You can use this method with "items.0.index.status" as the subKey, and 200 as the value.
//     * This will be applied for the test with the provided named in the project.
//     * </p>
//     * {@link ObjectMapper#convertValue(java.lang.Object, java.lang.Class)} is used to convert the value into to the appropriate JSON type.
//     *
//     * @param subKey   The direct child key of the "match" object to replace
//     * @param value    The value use to replace the existing value
//     * @param testName The name of the test for which this replacement should happen
//     */
    public void replaceMatch(String subKey, Object value, String testName) {
        ObjectNode replacementNode = new ObjectNode(jsonNodeFactory);
        replacementNode.set(subKey, MAPPER.convertValue(value, JsonNode.class));
        transformations.add(new ReplaceMatch(subKey, replacementNode, testName));
        transformationsKey.append("replace_match").append("subKey").append(testName).append(replacementNode.toString());
    }

//    @Input
//    public StringBuilder getTransformationsKey() {
//        return transformationsKey;
//    }

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
