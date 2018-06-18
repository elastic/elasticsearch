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

import java.io.IOException;

import static org.elasticsearch.common.xcontent.json.JsonXContent.jsonXContent;

public class JsonLogFileStructureFactory implements LogFileStructureFactory {

    private final Terminal terminal;

    public JsonLogFileStructureFactory(Terminal terminal) {
        this.terminal = terminal;
    }

    /**
     * This format matches if the sample consists of one or more JSON documents.
     * If there is more than one, they must be newline-delimited.  The
     * documents must be non-empty, to prevent lines containing "{}" from matching.
     */
    @Override
    public boolean canCreateFromSample(String sample) {
        try {
            String[] sampleLines = sample.split("\n");
            for (String sampleLine : sampleLines) {
                XContentParser parser =
                    jsonXContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, sampleLine);
                if (parser.map().isEmpty()) {
                    terminal.println(Verbosity.VERBOSE, "Not JSON because an empty object was parsed: [" + sampleLine + "]");
                    return false;
                }
                if (parser.nextToken() != null) {
                    terminal.println(Verbosity.VERBOSE, "Not newline delimited JSON because a line contained more than a single object: [" +
                        sampleLine + "]");
                    return false;
                }
            }
            return true;
        } catch (IOException | IllegalStateException e) {
            terminal.println(Verbosity.VERBOSE, "Not JSON because there was a parsing exception: [" + e.getMessage() + "]");
            return false;
        }
    }

    @Override
    public LogFileStructure createFromSample(String sampleFileName, String indexName, String typeName, String sample, String charsetName)
        throws IOException {
        return new JsonLogFileStructure(terminal, sampleFileName, indexName, typeName, sample, charsetName);
    }
}
