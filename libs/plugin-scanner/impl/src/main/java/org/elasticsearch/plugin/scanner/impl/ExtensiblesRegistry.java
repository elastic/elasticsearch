/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner.impl;

import org.elasticsearch.plugin.api.Extensible;

import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

public class ExtensiblesRegistry {

    // private static final Logger logger = LogManager.getLogger(ExtensiblesRegistry.class);

    private final ClassScanner extensibleClassScanner = new ClassScanner(Extensible.class, (classname, map) -> {
        map.put(classname, classname);
        return null;
    });

    // only 1 file for now, but in the future multiple for different apis? does this have to be in exported package?
    private static final String EXTENSIBLES_FILE = "extensibles.json";

    private final ExtensibleFileReader extensibleFileReader;

    ExtensiblesRegistry(Stream<Path> moduleOrClassPath) {
        this(EXTENSIBLES_FILE, moduleOrClassPath);
    }

    ExtensiblesRegistry(String extensiblesFile, Stream<Path> moduleOrClassPath) {
        extensibleFileReader = new ExtensibleFileReader(extensiblesFile);

        Map<String, String> fromFile = extensibleFileReader.readFromFile();
        if (fromFile.size() > 0) {
            // logger.info(() -> Strings.format("Loaded extensible from cache file %s", (fromFile)));
            // System.out.println("loaded " + fromFile);
            extensibleClassScanner.addFoundClasses(fromFile);
        } else {
            extensibleClassScanner.visit(ClassReaders.ofPaths(moduleOrClassPath));
        }

    }

    ClassScanner getExtensibleClassScanner() {
        return extensibleClassScanner;
    }
}
