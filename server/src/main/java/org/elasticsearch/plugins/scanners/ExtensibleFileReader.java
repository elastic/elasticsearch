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
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.XContentType.JSON;

public class ExtensibleFileReader {
    private static final Logger logger = LogManager.getLogger(ExtensibleFileReader.class);

    private String extensibleFile;

    public ExtensibleFileReader(String extensibleFile) {
        this.extensibleFile = extensibleFile;
    }

    public Map<String, String> readFromFile() {
        Map<String, String> res = new HashMap<>();
        try (InputStream in = /*new BufferedInputStream(*/getClass().getClassLoader().getResourceAsStream(extensibleFile))/*)*/ {
            if (in != null) {
                try (XContentParser parser = JSON.xContent().createParser(XContentParserConfiguration.EMPTY, in)) {
                    // validation class exist??
                    return parser.mapStrings()
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(e -> classNameToSlashes(e.getKey()), e -> classNameToSlashes(e.getValue())));
                    // todo decide if dot or /classes names should be used
                }
            }

        } catch (IOException e) {
            logger.error("failed reading extensible file", e);
        }
        return res;
    }

    // todo duplication
    private static String classNameToSlashes(String className) {
        return className.replace('.', '/');
    }
}
