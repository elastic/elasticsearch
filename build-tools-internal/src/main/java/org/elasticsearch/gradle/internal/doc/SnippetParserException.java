/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.doc;

import org.gradle.api.InvalidUserDataException;

import java.io.File;

public class SnippetParserException extends RuntimeException {
    private final File file;
    private final int lineNumber;

    public SnippetParserException(String message, Throwable cause) {
        super(message, cause);
        this.file = null;
        this.lineNumber = -1;
    }

    public SnippetParserException(File file, int lineNumber, InvalidUserDataException e) {
        super("Error parsing snippet in " + file.getName() + " at line " + lineNumber, e);
        this.file = file;
        this.lineNumber = lineNumber;
    }

    public File getFile() {
        return file;
    }

    public int getLineNumber() {
        return lineNumber;
    }
}
