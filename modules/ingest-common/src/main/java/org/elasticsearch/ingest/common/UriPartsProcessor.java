/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.web.UriParts;

import java.util.Map;

public class UriPartsProcessor extends AbstractProcessor {

    public static final String TYPE = "uri_parts";

    private final String field;
    private final String targetField;
    private final boolean removeIfSuccessful;
    private final boolean keepOriginal;
    private final boolean ignoreMissing;

    UriPartsProcessor(
        String tag,
        String description,
        String field,
        String targetField,
        boolean removeIfSuccessful,
        boolean keepOriginal,
        boolean ignoreMissing
    ) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.removeIfSuccessful = removeIfSuccessful;
        this.keepOriginal = keepOriginal;
        this.ignoreMissing = ignoreMissing;
    }

    public String getField() {
        return field;
    }

    public String getTargetField() {
        return targetField;
    }

    public boolean getRemoveIfSuccessful() {
        return removeIfSuccessful;
    }

    public boolean getKeepOriginal() {
        return keepOriginal;
    }

    public Object getIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        String value = ingestDocument.getFieldValue(field, String.class, ignoreMissing);

        if (ignoreMissing && null == value) {
            return ingestDocument;
        }
        var uriParts = apply(value);
        if (keepOriginal) {
            uriParts.put("original", value);
        }

        if (removeIfSuccessful && targetField.equals(field) == false) {
            ingestDocument.removeField(field);
        }
        ingestDocument.setFieldValue(targetField, uriParts);
        return ingestDocument;
    }

    public static Map<String, Object> apply(String urlString) {
        return UriParts.parse(urlString);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public UriPartsProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", "url");
            boolean removeIfSuccessful = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "remove_if_successful", false);
            boolean keepOriginal = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "keep_original", true);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            return new UriPartsProcessor(processorTag, description, field, targetField, removeIfSuccessful, keepOriginal, ignoreMissing);
        }
    }
}
