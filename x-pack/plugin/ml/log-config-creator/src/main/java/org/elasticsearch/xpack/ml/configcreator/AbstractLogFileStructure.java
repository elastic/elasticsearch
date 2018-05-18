/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.cli.Terminal;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.AccessControlException;
import java.util.Locale;
import java.util.Objects;
import java.util.SortedMap;
import java.util.stream.Collectors;

public abstract class AbstractLogFileStructure {

    protected static final String DEFAULT_TIMESTAMP_FIELD = "@timestamp";

    private static final String FIELD_MAPPING_TEMPLATE = "        \"%s\": {\n" +
        "          \"type\": \"%s\"\n" +
        "        }";
    private static final String INDEX_MAPPINGS_TEMPLATE = "PUT %s\n" +
        "{\n" +
        "  \"mappings\": {\n" +
        "    \"_doc\": {\n" +
        "      \"properties\": {\n" +
        "%s\n" +
        "      }\n" +
        "    }\n" +
        "  }\n" +
        "}\n";

    protected final Terminal terminal;
    protected final String sampleFileName;
    protected final String indexName;
    protected final String typeName;

    protected AbstractLogFileStructure(Terminal terminal, String sampleFileName, String indexName, String typeName) {
        this.terminal = Objects.requireNonNull(terminal);
        this.sampleFileName = Objects.requireNonNull(sampleFileName);
        this.indexName = Objects.requireNonNull(indexName);
        this.typeName = Objects.requireNonNull(typeName);
    }

    protected void writeConfigFile(Path directory, String fileName, String contents) throws IOException {
        Path fullPath = directory.resolve(typeName + "-" + fileName);
        Files.write(fullPath, contents.getBytes(StandardCharsets.UTF_8));
        terminal.println("Wrote config file " + fullPath);
        try {
            Files.setPosixFilePermissions(fullPath, PosixFilePermissions.fromString(fileName.endsWith(".sh") ? "rwxr-xr-x" : "rw-r--r--"));
        } catch (AccessControlException | UnsupportedOperationException e) {
            // * For AccessControlException, assume we're running in an ESTestCase unit test, which will have security manager enabled.
            // * For UnsupportedOperationException, assume we're on Windows.
            // In neither situation is it a problem that the file permissions can't be set.
        }
    }

    protected void writeRestCallConfigs(Path directory, String consoleFileName, String consoleCommand) throws IOException {
        writeConfigFile(directory, consoleFileName, consoleCommand);
        String curlCommand = "curl -H 'Content-Type: application/json' -X " +
            consoleCommand.replaceFirst(" ", " http://localhost:9200/").replaceFirst("\n", " -d '\n") + "'\n";
        writeConfigFile(directory, consoleFileName.replaceFirst("\\.console$", ".sh"), curlCommand);
    }

    protected void writeMappingsConfigs(Path directory, SortedMap<String, String> fieldTypes) throws IOException {
        String fieldTypeMappings = fieldTypes.entrySet().stream().map(entry -> String.format(Locale.ROOT, FIELD_MAPPING_TEMPLATE,
            entry.getKey(), entry.getValue())).collect(Collectors.joining(",\n"));
        writeRestCallConfigs(directory, "index-mappings.console", String.format(Locale.ROOT, INDEX_MAPPINGS_TEMPLATE, indexName,
            fieldTypeMappings));
    }

    protected static String bestLogstashQuoteFor(String str) {
        return (str.indexOf('"') >= 0) ? "'" : "\""; // NB: fails if field name contains both types of quotes
    }
}
