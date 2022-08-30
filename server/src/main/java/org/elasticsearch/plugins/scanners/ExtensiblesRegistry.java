/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners;

import org.elasticsearch.plugin.api.Extensible;

import java.io.IOException;
import java.util.Map;

public class ExtensiblesRegistry {
    public static final ClassScanner INSTANCE = new ClassScanner(Extensible.class, (classname, map) -> {
        map.put(classname, classname);
        return null;
    });

    static {
        try {
            INSTANCE.visit(ClassReaders.ofModuleAndClassPaths());
        } catch (IOException e) {
            // e.printStackTrace();
        }
    }

    public static Map<String, String> getScannedExtensibles() {
        return null;// NSTANCE.getExtensibleClasses();
    }

}
