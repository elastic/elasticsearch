/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.plugin.api.Extensible;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;

public class ExtensiblesRegistry {

    private static final Logger logger = LogManager.getLogger(ExtensiblesRegistry.class);

    private final ClassScanner extensibleClassScanner = new ClassScanner(Extensible.class, (classname, map) -> {
        map.put(classname, classname);
        return null;
    });

    // only 1 file for now, but in the future multiple for different apis? does this have to be in exported package?
    private static final String EXTENSIBLES_FILE = "extensibles.json";
    public static final ExtensiblesRegistry INSTANCE = new ExtensiblesRegistry(EXTENSIBLES_FILE);

    private final ExtensibleFileReader extensibleFileReader;

    ExtensiblesRegistry(String extensiblesFile) {
        extensibleFileReader = new ExtensibleFileReader(extensiblesFile);

        try {
            Map<String, String> fromFile = extensibleFileReader.readFromFile();
            if (fromFile.size() > 0) {
                logger.debug(() -> format("Loaded extensible from cache file %s", (fromFile)));
                extensibleClassScanner.addFoundClasses(fromFile);
            } else {
                extensibleClassScanner.visit(ClassReaders.ofModuleAndClassPaths());
            }
        } catch (IOException e) {
            logger.error("Unable to load extensible classes", e);
        }
    }

    ClassScanner getExtensibleClassScanner() {
        return extensibleClassScanner;
    }
}
