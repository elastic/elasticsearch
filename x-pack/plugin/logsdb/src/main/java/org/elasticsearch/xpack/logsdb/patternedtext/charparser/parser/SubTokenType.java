/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext.charparser.parser;

import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.EncodingType;
import org.elasticsearch.xpack.logsdb.patternedtext.charparser.common.TimestampComponentType;

public final class SubTokenType extends ParsingType {
    final TimestampComponentType timestampComponentType;

    public SubTokenType(
        String name,
        EncodingType encodingType,
        int[] tokenBitmaskByPosition,
        TimestampComponentType timestampComponentType
    ) {
        super(name, encodingType, 1, null, tokenBitmaskByPosition);
        this.timestampComponentType = timestampComponentType;
    }

    public TimestampComponentType getTimestampComponentType() {
        return timestampComponentType;
    }
}
