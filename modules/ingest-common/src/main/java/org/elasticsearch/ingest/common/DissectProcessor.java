/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.dissect.DissectParser;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.Map;

public final class DissectProcessor extends AbstractProcessor {

    public static final String TYPE = "dissect";
    //package private members for testing
    final String field;
    final boolean ignoreMissing;
    final String pattern;
    final String appendSeparator;
    final DissectParser dissectParser;

    DissectProcessor(String tag, String description, String field, String pattern, String appendSeparator, boolean ignoreMissing) {
        super(tag, description);
        this.field = field;
        this.ignoreMissing = ignoreMissing;
        this.pattern = pattern;
        this.appendSeparator = appendSeparator;
        this.dissectParser = new DissectParser(pattern, appendSeparator);
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        String input = ingestDocument.getFieldValue(field, String.class, ignoreMissing);
        if (input == null && ignoreMissing) {
            return ingestDocument;
        } else if (input == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot process it.");
        }
        dissectParser.forceParse(input).forEach(ingestDocument::setFieldValue);
        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public DissectProcessor create(Map<String, Processor.Factory> registry, String processorTag, String description,
                                       Map<String, Object> config) {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String pattern = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "pattern");
            String appendSeparator = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "append_separator", "");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            return new DissectProcessor(processorTag, description, field, pattern, appendSeparator, ignoreMissing);
        }
    }
}
