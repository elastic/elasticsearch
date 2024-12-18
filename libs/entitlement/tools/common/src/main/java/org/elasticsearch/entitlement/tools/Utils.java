/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.tools;

import java.io.IOException;
import java.lang.module.ModuleDescriptor;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Utils {

    public static Map<String, Set<String>> findModuleExports(FileSystem fs) throws IOException {
        var modulesExports = new HashMap<String, Set<String>>();
        try (var stream = Files.walk(fs.getPath("modules"))) {
            stream.filter(p -> p.getFileName().toString().equals("module-info.class")).forEach(x -> {
                try (var is = Files.newInputStream(x)) {
                    var md = ModuleDescriptor.read(is);
                    modulesExports.put(
                        md.name(),
                        md.exports()
                            .stream()
                            .filter(e -> e.isQualified() == false)
                            .map(ModuleDescriptor.Exports::source)
                            .collect(Collectors.toSet())
                    );
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        return modulesExports;
    }

}
