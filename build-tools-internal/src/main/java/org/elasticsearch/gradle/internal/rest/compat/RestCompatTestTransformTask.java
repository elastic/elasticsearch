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
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.test.rest.transform.RestTestTransform;
import org.elasticsearch.gradle.internal.test.rest.transform.RestTestTransformer;
import org.elasticsearch.gradle.internal.test.rest.transform.do_.ReplaceKeyInDo;
import org.elasticsearch.gradle.internal.test.rest.transform.headers.InjectHeaders;
import org.elasticsearch.gradle.internal.test.rest.transform.length.ReplaceKeyInLength;
import org.elasticsearch.gradle.internal.test.rest.transform.match.AddMatch;
import org.elasticsearch.gradle.internal.test.rest.transform.match.RemoveMatch;
import org.elasticsearch.gradle.internal.test.rest.transform.match.ReplaceKeyInMatch;
import org.elasticsearch.gradle.internal.test.rest.transform.match.ReplaceValueInMatch;
import org.elasticsearch.gradle.internal.test.rest.transform.text.ReplaceIsFalse;
import org.elasticsearch.gradle.internal.test.rest.transform.text.ReplaceIsTrue;
import org.elasticsearch.gradle.internal.test.rest.transform.text.ReplaceTextual;
import org.elasticsearch.gradle.internal.test.rest.transform.warnings.InjectAllowedWarnings;
import org.elasticsearch.gradle.internal.test.rest.transform.warnings.InjectWarnings;
import org.elasticsearch.gradle.internal.test.rest.transform.warnings.RemoveWarnings;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.FileTree;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Nested;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A task to transform REST tests for use in REST API compatibility before they are executed.
 */
public class RestCompatTestTransformTask extends DefaultTask {

    private static final YAMLFactory YAML_FACTORY = new YAMLFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper(YAML_FACTORY);
    private static final ObjectReader READER = MAPPER.readerFor(ObjectNode.class);
    private static final ObjectWriter WRITER = MAPPER.writerFor(ObjectNode.class);
    private static final String REST_TEST_PREFIX = "rest-api-spec/test";

    private static final Map<String, String> headers = new LinkedHashMap<>();

    private final FileSystemOperations fileSystemOperations;
    private final int compatibleVersion;
    private final DirectoryProperty sourceDirectory;
    private final DirectoryProperty outputDirectory;
    private final PatternFilterable testPatternSet;
    private final List<RestTestTransform<?>> transformations = new ArrayList<>();

    @Inject
    public RestCompatTestTransformTask(
        FileSystemOperations fileSystemOperations,
        Factory<PatternSet> patternSetFactory,
        ObjectFactory objectFactory
    ) {
        this.fileSystemOperations = fileSystemOperations;
        this.compatibleVersion = Version.fromString(VersionProperties.getVersions().get("elasticsearch")).getMajor() - 1;
        this.sourceDirectory = objectFactory.directoryProperty();
        this.outputDirectory = objectFactory.directoryProperty();
        this.testPatternSet = patternSetFactory.create();
        this.testPatternSet.include("/*" + "*/*.yml"); // concat these strings to keep build from thinking this is invalid javadoc
        // always inject compat headers
        headers.put("Content-Type", "application/vnd.elasticsearch+json;compatible-with=" + compatibleVersion);
        headers.put("Accept", "application/vnd.elasticsearch+json;compatible-with=" + compatibleVersion);
        transformations.add(new InjectHeaders(headers, Set.of(RestCompatTestTransformTask::doesNotHaveCatOperation)));
    }

    private static boolean doesNotHaveCatOperation(ObjectNode doNodeValue) {
        final Iterator<String> fieldNamesIterator = doNodeValue.fieldNames();
        while (fieldNamesIterator.hasNext()) {
            final String fieldName = fieldNamesIterator.next();
            if (fieldName.startsWith("cat.")) {
                return false;
            }
        }
        return true;
    }

    /**
     * Replaces all the values of a match assertion for all project REST tests.
     * For example "match":{"_type": "foo"} to "match":{"_type": "bar"}
     *
     * @param subKey the key name directly under match to replace. For example "_type"
     * @param value  the value used in the replacement. For example "bar"
     */
    public void replaceValueInMatch(String subKey, Object value) {
        transformations.add(new ReplaceValueInMatch(subKey, MAPPER.convertValue(value, JsonNode.class)));
    }

    /**
     * Replaces the values of a match assertion for the given REST test. For example "match":{"_type": "foo"} to "match":{"_type": "bar"}
     *
     * @param subKey   the key name directly under match to replace. For example "_type"
     * @param value    the value used in the replacement. For example "bar"
     * @param testName the testName to apply replacement
     */
    public void replaceValueInMatch(String subKey, Object value, String testName) {
        transformations.add(new ReplaceValueInMatch(subKey, MAPPER.convertValue(value, JsonNode.class), testName));
    }

    /**
     * A transformation to replace the key in a do section.
     * @see ReplaceKeyInDo
     * @param oldKeyName   the key name directly under do to replace.
     * @param newKeyName   the new key name directly under do.
     */
    public void replaceKeyInDo(String oldKeyName, String newKeyName) {
        transformations.add(new ReplaceKeyInDo(oldKeyName, newKeyName, null));
    }

    /**
     * A transformation to replace the key in a length assertion.
     * @see ReplaceKeyInLength
     * @param oldKeyName   the key name directly under length to replace.
     * @param newKeyName   the new key name directly under length.
     */
    public void replaceKeyInLength(String oldKeyName, String newKeyName) {
        transformations.add(new ReplaceKeyInLength(oldKeyName, newKeyName, null));
    }

    /**
     * A transformation to replace the key in a match assertion.
     * @see ReplaceKeyInMatch
     * @param oldKeyName   the key name directly under match to replace.
     * @param newKeyName   the new key name directly under match.
     */
    public void replaceKeyInMatch(String oldKeyName, String newKeyName) {
        transformations.add(new ReplaceKeyInMatch(oldKeyName, newKeyName, null));
    }

    /**
     * Replaces all the values of a is_true assertion for all project REST tests.
     * For example "is_true": "value_to_replace" to "is_true": "value_replaced"
     *
     * @param oldValue the value that has to match and will be replaced
     * @param newValue  the value used in the replacement
     */
    public void replaceIsTrue(String oldValue, Object newValue) {
        transformations.add(new ReplaceIsTrue(oldValue, MAPPER.convertValue(newValue, TextNode.class)));
    }

    /**
     * Replaces all the values of a is_false assertion for all project REST tests.
     * For example "is_false": "value_to_replace" to "is_false": "value_replaced"
     *
     * @param oldValue the value that has to match and will be replaced
     * @param newValue  the value used in the replacement
     */
    public void replaceIsFalse(String oldValue, Object newValue) {
        transformations.add(new ReplaceIsFalse(oldValue, MAPPER.convertValue(newValue, TextNode.class)));
    }

    /**
     * Replaces all the values of a is_false assertion for given REST test.
     * For example "is_false": "value_to_replace" to "is_false": "value_replaced"
     *
     * @param oldValue the value that has to match and will be replaced
     * @param newValue  the value used in the replacement
      @param testName the testName to apply replacement
     */
    public void replaceIsFalse(String oldValue, Object newValue, String testName) {
        transformations.add(new ReplaceIsFalse(oldValue, MAPPER.convertValue(newValue, TextNode.class), testName));
    }

    /**
     * Replaces all the values of a given key/value pairs for all project REST tests.
     * For example "foo": "bar" can replaced as "foo": "baz"
     *
     * @param key the key to find
     * @param oldValue the value of that key to find
     * @param newValue  the value used in the replacement
     */
    public void replaceValueTextByKeyValue(String key, String oldValue, Object newValue) {
        transformations.add(new ReplaceTextual(key, oldValue, MAPPER.convertValue(newValue, TextNode.class)));
    }

    /**
     * Replaces all the values of a given key/value pairs for for given REST test.
     * For example "foo": "bar" can replaced as "foo": "baz"
     *
     * @param key the key to find
     * @param oldValue the value of that key to find
     * @param newValue  the value used in the replacement
     * @param testName the testName to apply replacement
     */
    public void replaceValueTextByKeyValue(String key, String oldValue, Object newValue, String testName) {
        transformations.add(new ReplaceTextual(key, oldValue, MAPPER.convertValue(newValue, TextNode.class), testName));
    }


    /**
     * Removes the key/value of a match assertion all project REST tests for the matching subkey.
     * For example "match":{"_type": "foo"} to "match":{}
     * An empty match is retained if there is only a single key under match.
     *
     * @param subKey the key name directly under match to replace. For example "_type"
     */
    public void removeMatch(String subKey) {
        transformations.add(new RemoveMatch(subKey));
    }

    /**
     * Removes the key/value of a match assertion for the given REST tests for the matching subkey.
     * For example "match":{"_type": "foo"} to "match":{}
     * An empty match is retained if there is only a single key under match.
     *
     * @param subKey   the key name directly under match to remove. For example "_type"
     * @param testName the testName to apply removal
     */
    public void removeMatch(String subKey, String testName) {
        transformations.add(new RemoveMatch(subKey, testName));
    }

    /**
     * Adds a match assertion for the given REST test. For example add "match":{"_type": "foo"} to the test.
     *
     * @param subKey   the key name directly under match to add. For example "_type"
     * @param value    the value used in the addition. For example "foo"
     * @param testName the testName to apply addition
     */
    public void addMatch(String subKey, Object value, String testName) {
        transformations.add(new AddMatch(subKey, MAPPER.convertValue(value, JsonNode.class), testName));
    }

    /**
     * Adds one or more warnings to the given test
     * @param testName the test name to add the warning
     * @param warnings the warning(s) to add
     */
    public void addWarning(String testName, String... warnings) {
        transformations.add(new InjectWarnings(Arrays.asList(warnings), testName));
    }

    /**
     * Adds one or more regex warnings to the given test
     * @param testName the test name to add the regex warning
     * @param warningsRegex the regex warning(s) to add
     */
    public void addWarningRegex(String testName, String... warningsRegex) {
        transformations.add(new InjectWarnings(true, Arrays.asList(warningsRegex), testName));
    }

    /**
     * Removes one or more warnings
     * @param warnings the warning(s) to remove
     */
    public void removeWarning(String... warnings) {
        transformations.add(new RemoveWarnings(Set.copyOf(Arrays.asList(warnings))));
    }

    /**
     * Removes one or more warnings
     * @param warnings the warning(s) to remove
     * @param testName the test name to remove the warning
     */
    public void removeWarningForTest(String warnings, String testName) {
        transformations.add(new RemoveWarnings(Set.copyOf(Arrays.asList(warnings)), testName));
    }

    /**
     * Adds one or more allowed warnings
     * @param allowedWarnings the warning(s) to add
     */
    public void addAllowedWarning(String... allowedWarnings) {
        transformations.add(new InjectAllowedWarnings(Arrays.asList(allowedWarnings)));
    }

    /**
     * Adds one or more allowed regular expression warnings
     * @param allowedWarningsRegex the regex warning(s) to add
     */
    public void addAllowedWarningRegex(String... allowedWarningsRegex) {
        transformations.add(new InjectAllowedWarnings(true, Arrays.asList(allowedWarningsRegex)));
    }

    /**
     * Adds one or more allowed regular expression warnings
     * @param allowedWarningsRegex the regex warning(s) to add
     * @testName the test name to add a allowedWarningRegex
     */
    public void addAllowedWarningRegexForTest(String allowedWarningsRegex, String testName) {
        transformations.add(new InjectAllowedWarnings(true, Arrays.asList(allowedWarningsRegex), testName));
    }

    @OutputDirectory
    public DirectoryProperty getOutputDirectory() {
        return outputDirectory;
    }

    @SkipWhenEmpty
    @InputFiles
    public FileTree getTestFiles() {
        return sourceDirectory.getAsFileTree().matching(testPatternSet);
    }

    @TaskAction
    public void transform() throws IOException {
        // clean the output directory to ensure no stale files persist
        fileSystemOperations.delete(d -> d.delete(outputDirectory));

        RestTestTransformer transformer = new RestTestTransformer();
        // TODO: instead of flattening the FileTree here leverage FileTree.visit() so we can preserve folder hierarchy in a more robust way
        for (File file : getTestFiles().getFiles()) {
            YAMLParser yamlParser = YAML_FACTORY.createParser(file);
            List<ObjectNode> tests = READER.<ObjectNode>readValues(yamlParser).readAll();
            List<ObjectNode> transformRestTests = transformer.transformRestTests(new LinkedList<>(tests), transformations);
            // convert to url to ensure forward slashes
            String[] testFileParts = file.toURI().toURL().getPath().split(REST_TEST_PREFIX);
            if (testFileParts.length != 2) {
                throw new IllegalArgumentException("could not split " + file + " into expected parts");
            }
            File output = new File(outputDirectory.get().dir(REST_TEST_PREFIX).getAsFile(), testFileParts[1]);
            output.getParentFile().mkdirs();
            try (SequenceWriter sequenceWriter = WRITER.writeValues(output)) {
                for (ObjectNode transformedTest : transformRestTests) {
                    sequenceWriter.write(transformedTest);
                }
            }
        }
    }

    @Internal
    public DirectoryProperty getSourceDirectory() {
        return sourceDirectory;
    }

    @Nested
    public List<RestTestTransform<?>> getTransformations() {
        return transformations;
    }
}
