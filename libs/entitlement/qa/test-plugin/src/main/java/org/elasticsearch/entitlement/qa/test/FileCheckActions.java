/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import org.elasticsearch.core.SuppressForbidden;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

@SuppressForbidden(reason = "Explicitly checking APIs that are forbidden")
class FileCheckActions {

    static void createScanner() throws FileNotFoundException {
        new Scanner(new File(""));
    }

    static void createScannerWithCharset() throws IOException {
        new Scanner(new File(""), StandardCharsets.UTF_8);
    }

    static void createScannerWithCharsetName() throws FileNotFoundException {
        new Scanner(new File(""), "UTF-8");
    }

    private FileCheckActions() {}
}
