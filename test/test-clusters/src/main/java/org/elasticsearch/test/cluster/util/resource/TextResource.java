/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster.util.resource;

import java.io.File;

public interface TextResource {
    String getText();

    static TextResource fromString(String text) {
        return new StringTextResource(text);
    }

    static TextResource fromClasspath(String path) {
        return new ClasspathTextResource(path);
    }

    static TextResource fromFile(File file) {
        return new FileTextResource(file);
    }
}
