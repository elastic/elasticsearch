/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.scanner;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.stream.Stream;

//todo api note
public class ClassUtil {
    // try catch
    public static Stream<Path> ofClassPath() throws IOException {
        String classpath = System.getProperty("java.class.path");
        return ofClassPath(classpath);
    }

    @SuppressForbidden(reason = "converting classpath")
    // try catch
    static Stream<Path> ofClassPath(String classpath) {
        if (classpath != null && classpath.equals("") == false) {// todo when do we set cp to "" ?
            String[] pathelements = classpath.split(":");
            return Arrays.stream(pathelements).map(PathUtils::get);
        }
        return Stream.empty();
    }

    // try catch
    public static Stream<Path> ofModulePath() throws IOException {
        // TODO should we use this? or should we use Environment#modulePath?
        String modulePath = System.getProperty("jdk.module.path");
        return ofDirWithJars(modulePath);
    }

    @SuppressForbidden(reason = "converting modulePaths")
    // try catch
    public static Stream<Path> ofDirWithJars(String path) throws IOException {
        if (path == null) {
            return Stream.empty();
        }
        Path dir = PathUtils.get(path);
        return Files.list(dir);
    }

}
