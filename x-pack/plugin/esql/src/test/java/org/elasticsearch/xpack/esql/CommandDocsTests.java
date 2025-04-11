/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.junit.AfterClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * This test class is used to generate examples for the commands documentation.
 */
public class CommandDocsTests extends ESTestCase {
    public void testDummy() {
        assert true;
    }

    @AfterClass
    public static void renderDocs() throws IOException {
        if (System.getProperty("generateDocs") == null) {
            return;
        }
        TestDocsV3Support docs = new TestDocsV3Support();
        docs.renderExamples();
    }

    public static Map<List<DataType>, DataType> signatures() {
        // The cast operator cannot produce sensible signatures unless we consider the type as an extra parameter
        return Map.of();
    }

    /**
     * This class only exists to provide access to the examples rendering capabilities.
     * It implements the normal renderSignature and renderDocs methods with empty implementations.
     * Instead, it has a new renderExamples method that loads all the examples based on a file written by the gradle task.
     * This task finds all files in esql/_snippets/commands/examples/*.csv-spec/*.md and writes them to a file in the temp directory.
     * This task will then overwrite those files with the current examples, so that changes to the examples will be reflected
     * in the documentation.
     */
    public static class TestDocsV3Support extends DocsV3Support {
        public TestDocsV3Support() {
            super("commands", "commands", CommandDocsTests.class, null);
        }

        @Override
        protected void renderSignature() throws IOException {
            // Not implemented
        }

        @Override
        protected void renderDocs() throws IOException {
            // Not implemented
        }

        protected void renderExamples() throws IOException {
            // We want to render all examples for the commands documentation
            // Load all files from the temporary directory used by tests
            int count = 0;
            for (String line : readFromTempDir("commands.examples")) {
                if (renderExample(line)) {
                    count++;
                }
            }
            logger.info("Rendered {} examples", count);
        }

        protected boolean renderExample(String lineSpec) throws IOException {
            // We read this from a file written by gradle, derived from the existing sample files in the commands documentation
            // Each line should have the format: drop.csv-spec/heightWithWildcard.md
            // In other words, the csv-spec file as a directory, and the examples tag as a file name
            String[] parts = lineSpec.split("/");
            if (parts.length != 2) {
                logger.error("Invalid example specification: {}", lineSpec);
                return false;
            }
            String csvFile = parts[0];
            String tagFile = parts[1];
            String tag = tagFile.substring(0, tagFile.lastIndexOf('.'));
            if (csvFile.endsWith(".csv-spec") == false) {
                logger.error("Invalid example specification, csv-spec file must end with csv-spec: {}", csvFile);
                return false;
            }
            StringBuilder builder = new StringBuilder();
            builder.append(DOCS_WARNING);
            String exampleQuery = loadExample(csvFile, tag);
            if (exampleQuery == null) {
                logger.error("Failed to load example [{}:{}]", csvFile, tag);
                return false;
            }
            builder.append("```esql\n").append(exampleQuery).append("\n```\n");
            String exampleResult = loadExample(csvFile, tag + "-result");
            if (exampleResult == null) {
                logger.warn("Failed to load example result [{}:{}]", csvFile, tag + "-result");
            } else {
                builder.append("\n").append(exampleResult);
            }
            String rendered = builder.toString();
            logger.info("Writing example for [{}]:\n{}", name, rendered);
            writeExampleFile(csvFile, tagFile, rendered);
            return true;
        }

        protected void writeExampleFile(String csvFile, String tagFile, String str) throws IOException {
            // We have to write to a tempdir because it’s all test are allowed to write to. Gradle can move them.
            Path dir = PathUtils.get(System.getProperty("java.io.tmpdir"))
                .resolve("esql")
                .resolve("_snippets")
                .resolve(category)
                .resolve("examples")
                .resolve(csvFile);
            Files.createDirectories(dir);
            Path file = dir.resolve(tagFile);
            Files.writeString(file, str);
            logger.info("Wrote to file: {}", file);
        }

        @SuppressWarnings("SameParameterValue")
        protected List<String> readFromTempDir(String filename) throws IOException {
            // We have to read from a tempdir because it’s all test are allowed to read to. Gradle can write this file before the tests
            Path file = PathUtils.get(System.getProperty("java.io.tmpdir")).resolve("esql").resolve(filename);
            if (Files.exists(file)) {
                logger.info("Reading commands examples file: {}", file);
                List<String> lines = Files.readAllLines(file);
                logger.info("Read {} examples specifications from: {}", lines.size(), file);
                return lines;
            } else {
                logger.info("Examples file missing: {}", file);
                throw new IllegalArgumentException("Examples file missing: " + file);
            }
        }
    }

}
