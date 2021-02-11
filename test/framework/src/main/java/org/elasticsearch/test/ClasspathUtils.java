/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.common.io.PathUtils;

import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class ClasspathUtils {

    public static Path[] findPaths(ClassLoader classLoader, String path) throws Exception {
        Enumeration<URL> resources = classLoader.getResources(path);
        List<Path> paths = new ArrayList<>();

        while (resources.hasMoreElements()) {
            paths.add(PathUtils.get(resources.nextElement().toURI()));
        }

        return paths.toArray(new Path[]{});
    }
}
