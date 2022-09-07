/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner.impl;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


public class ExtensibleFileReader {
    private static final ObjectMapper mapper = new ObjectMapper();

//    private static final Logger logger = LogManager.getLogger(ExtensibleFileReader.class);

    private String extensibleFile;

    public ExtensibleFileReader(String extensibleFile) {
        this.extensibleFile = extensibleFile;
    }

    @SuppressWarnings("unchecked")
    public Map<String, String> readFromFile() {
        Map<String, String> res = new HashMap<>();
        try (InputStream in = /*new BufferedInputStream(*/getClass().getClassLoader().getResourceAsStream(extensibleFile))/*)*/ {
            if (in != null) {
               return ((Map<String,String>)mapper.readValue(in, Map.class)).entrySet()
                        .stream()
                        .collect(Collectors.toMap(e -> classNameToSlashes(e.getKey()), e -> classNameToSlashes(e.getValue())));
                // todo decide if dot or /classes names should be used
            }

        } catch (IOException e) {
//            logger.error("failed reading extensible file", e);
        }
        return res;
    }

    // todo duplication
    private static String classNameToSlashes(String className) {
        return className.replace('.', '/');
    }
}
