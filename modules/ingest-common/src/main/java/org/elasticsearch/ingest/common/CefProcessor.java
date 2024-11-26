/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.Map;

/**
 * A processor that breaks line from CSV file into separate fields.
 * If there's more fields requested than there is in the CSV, extra field will
 * not be present in the document after processing.
 * In the same way this processor will skip any field that is empty in CSV.
 * <p>
 * By default it uses rules according to
 * <a href="https://tools.ietf.org/html/rfc4180">RCF 4180</a> with one
 * exception: whitespaces are
 * allowed before or after quoted field. Processor can be tweaked with following
 * parameters:
 * <p>
 * quote: set custom quote character (defaults to ")
 * separator: set custom separator (defaults to ,)
 * trim: trim leading and trailing whitespaces in unquoted fields
 * empty_value: sets custom value to use for empty fields (field is skipped if
 * null)
 */
public final class CefProcessor extends AbstractProcessor {

    public static final String TYPE = "cef";

    // visible for testing
    final String field;
    final boolean ignoreMissing;
    final boolean removeEmptyValue;

    CefProcessor(
            String tag,
            String description,
            String field,
            String targetField,
            boolean ignoreMissing,
            boolean removeEmptyValue) {
        super(tag, description);
        this.field = field;
        this.ignoreMissing = ignoreMissing;
        this.removeEmptyValue = removeEmptyValue;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) {
        String line = ingestDocument.getFieldValue(field, String.class, ignoreMissing);
        if (line == null && ignoreMissing) {
            return ingestDocument;
        } else if (line == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot process it.");
        }
        new CefParser(ingestDocument, removeEmptyValue).process(line);
        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements org.elasticsearch.ingest.Processor.Factory {
        @Override
        public CefProcessor create(
                Map<String, Processor.Factory> registry,
                String processorTag,
                String description,
                Map<String, Object> config) {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");

            boolean removeEmptyValue = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config,
                    "ignore_empty_value", true);

            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing",
                    false);
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field");
            return new CefProcessor(processorTag, description, field, targetField, ignoreMissing, removeEmptyValue);
        }
    }
}