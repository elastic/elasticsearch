/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.Map;

/**
 * Processor that converts the content of string fields to the byte value.
 * Throws exception is the field is not of type string or can not convert to the numeric byte value
 */
public final class BytesProcessor extends AbstractStringProcessor<Long> {

    public static final String TYPE = "bytes";

    BytesProcessor(String processorTag, String description, String field, boolean ignoreMissing, String targetField) {
        super(processorTag, description, ignoreMissing, targetField, field);
    }

    public static long apply(String value) {
        return ByteSizeValue.parseBytesSizeValue(value, null, "Ingest Field").getBytes();
    }

    @Override
    protected Long process(String value) {
        return apply(value);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory extends AbstractStringProcessor.Factory {

        public Factory() {
            super(TYPE);
        }

        @Override
        protected BytesProcessor newProcessor(String tag, String description, Map<String, Object> config, String field,
                                              boolean ignoreMissing, String targetField) {
            return new BytesProcessor(tag, description, field, ignoreMissing, targetField);
        }
    }
}
