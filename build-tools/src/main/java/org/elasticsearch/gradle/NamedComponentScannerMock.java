/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class NamedComponentScannerMock {
    public static void main(String[] args) throws IOException {
        // expect a file name to passed in as a parameter
        // creating a file so that we can assert about this in a test
        Path path = Path.of(args[0]);
        Files.createDirectories(path.getParent());
        Files.createFile(path);
    }
}
