/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ingest;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class UriPartsProcessor extends AbstractProcessor {

    public static final String TYPE = "uri_parts";

    private final String field;
    private final String targetField;
    private final boolean removeIfSuccessful;
    private final boolean keepOriginal;

    UriPartsProcessor(String tag, String description, String field, String targetField, boolean removeIfSuccessful, boolean keepOriginal) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.removeIfSuccessful = removeIfSuccessful;
        this.keepOriginal = keepOriginal;
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

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        String value = ingestDocument.getFieldValue(field, String.class);

        URI uri;
        try {
            uri = new URI(value);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("unable to parse URI [" + value + "]");
        }
        var uriParts = new HashMap<String, Object>();
        uriParts.put("domain", uri.getHost());
        if (uri.getFragment() != null) {
            uriParts.put("fragment", uri.getFragment());
        }
        if (keepOriginal) {
            uriParts.put("original", value);
        }
        final String path = uri.getPath();
        if (path != null) {
            uriParts.put("path", path);
            if (path.contains(".")) {
                int periodIndex = path.lastIndexOf('.');
                uriParts.put("extension", periodIndex < path.length() ? path.substring(periodIndex + 1) : "");
            }
        }
        if (uri.getPort() != -1) {
            uriParts.put("port", uri.getPort());
        }
        if (uri.getQuery() != null) {
            uriParts.put("query", uri.getQuery());
        }
        uriParts.put("scheme", uri.getScheme());
        final String userInfo = uri.getUserInfo();
        if (userInfo != null) {
            uriParts.put("user_info", userInfo);
            if (userInfo.contains(":")) {
                int colonIndex = userInfo.indexOf(":");
                uriParts.put("username", userInfo.substring(0, colonIndex));
                uriParts.put("password", colonIndex < userInfo.length() ? userInfo.substring(colonIndex + 1) : "");
            }
        }

        if (removeIfSuccessful && targetField.equals(field) == false) {
            ingestDocument.removeField(field);
        }
        ingestDocument.setFieldValue(targetField, uriParts);
        return ingestDocument;
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
            Map<String, Object> config
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", "url");
            boolean removeIfSuccessful = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "remove_if_successful", false);
            boolean keepOriginal = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "keep_original", true);
            return new UriPartsProcessor(processorTag, description, field, targetField, removeIfSuccessful, keepOriginal);
        }
    }
}
