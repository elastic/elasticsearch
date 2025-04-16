/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.spi.annotation;

import java.util.Map;

public class NoImportAnnotationParser implements WhitelistAnnotationParser {

    public static final NoImportAnnotationParser INSTANCE = new NoImportAnnotationParser();

    private NoImportAnnotationParser() {

    }

    @Override
    public Object parse(Map<String, String> arguments) {
        if (arguments.isEmpty() == false) {
            throw new IllegalArgumentException("unexpected parameters for [@no_import] annotation, found " + arguments);
        }

        return NoImportAnnotation.INSTANCE;
    }
}
