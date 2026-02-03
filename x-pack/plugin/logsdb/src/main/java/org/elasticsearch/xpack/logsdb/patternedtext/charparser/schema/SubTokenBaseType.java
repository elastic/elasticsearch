/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.schema;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;

public record SubTokenBaseType(
    String name,
    EncodingType encodingType,
    String symbol,
    Class<?> baseType,
    String description,
    char[] allowedCharacters
) {

    @Override
    public String toString() {
        return name;
    }
}
