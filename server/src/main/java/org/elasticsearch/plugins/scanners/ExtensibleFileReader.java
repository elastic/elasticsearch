/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentType.JSON;

public class ExtensibleFileReader {
    private static final Logger logger = LogManager.getLogger(ExtensibleFileReader.class);

    private String extensibleFile;

    public ExtensibleFileReader(String extensibleFile) {
        this.extensibleFile = extensibleFile;
    }

    public Map<String, String> readFromFile() {
        Map<String, String> res = new HashMap<>();
        // todo should it be BufferedInputStream ?
        try (InputStream in = getClass().getResourceAsStream(extensibleFile)) {
            if (in != null) {
                try (XContentParser parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, in)) {
                    // TODO should we validate the classes actually exist?
                    return parser.mapStrings();
                }
            }
        } catch (IOException e) {
            logger.error("failed reading extensible file", e);
        }
        return res;
    }

}
