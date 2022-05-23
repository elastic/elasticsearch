/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.spi.annotation;

import java.util.Map;

/**
 * Parser for the <pre>@alias[class="Inner"]</pre> annotation.  See {@link AliasAnnotation} for details.
 */
public class AliasAnnotationParser implements WhitelistAnnotationParser {
    public static final AliasAnnotationParser INSTANCE = new AliasAnnotationParser();

    private AliasAnnotationParser() {}

    @Override
    public Object parse(Map<String, String> arguments) {
        if (arguments.size() != 1) {
            throw new IllegalArgumentException("[@alias] requires one alias");
        }
        AliasAnnotation annotation = null;
        for (Map.Entry<String, String> entry : arguments.entrySet()) {
            if ("class".equals(entry.getKey()) == false) {
                throw new IllegalArgumentException("[@alias] only supports class aliases");
            }
            String alias = entry.getValue();
            if (alias == null || alias.isBlank()) {
                throw new IllegalArgumentException("[@alias] must be non-empty");
            }
            annotation = new AliasAnnotation(alias);
        }
        return annotation;
    }
}
