/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;

/**
 * Processor that URL-decodes a string
 */
public final class URLDecodeProcessor extends AbstractStringProcessor<String> {

    public static final String TYPE = "urldecode";

    URLDecodeProcessor(String processorTag, String description, String field, boolean ignoreMissing, String targetField) {
        super(processorTag, description, ignoreMissing, targetField, field);
    }

    public static String apply(String value) {
        try {
            return URLDecoder.decode(value, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Could not URL-decode value.", e);
        }
    }

    @Override
    protected String process(String value) {
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
        protected URLDecodeProcessor newProcessor(
            String tag,
            String description,
            Map<String, Object> config,
            String field,
            boolean ignoreMissing,
            String targetField
        ) {
            return new URLDecodeProcessor(tag, description, field, ignoreMissing, targetField);
        }
    }
}
