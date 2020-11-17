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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class UrlPartsProcessor extends AbstractProcessor {

    public static final String TYPE = "url_parts";

    private final String field;
    private final String targetField;
    private final boolean removeIfSuccessful;
    private final boolean keepOriginal;

    UrlPartsProcessor(String tag, String description, String field, String targetField, boolean removeIfSuccessful, boolean keepOriginal) {
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

        URL url;
        try {
            url = new URL(value);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("unable to parse URL [" + value + "]");
        }
        var urlParts = new HashMap<String, Object>();
        urlParts.put("domain", url.getHost());
        if (url.getRef() != null) {
            urlParts.put("fragment", url.getRef());
        }
        if (keepOriginal) {
            urlParts.put("original", value);
        }
        final String path = url.toURI().getPath();
        if (path != null) {
            urlParts.put("path", path);
            if (path.contains(".")) {
                int periodIndex = path.lastIndexOf('.');
                urlParts.put("extension", periodIndex < path.length() ? path.substring(periodIndex + 1) : "");
            }
        }
        if (url.getPort() != -1) {
            urlParts.put("port", url.getPort());
        }
        if (url.getQuery() != null) {
            urlParts.put("query", url.getQuery());
        }
        urlParts.put("scheme", url.getProtocol());
        final String userInfo = url.getUserInfo();
        if (userInfo != null) {
            urlParts.put("user_info", userInfo);
            if (userInfo.contains(":")) {
                int colonIndex = userInfo.indexOf(":");
                urlParts.put("username", userInfo.substring(0, colonIndex));
                urlParts.put("password", colonIndex < userInfo.length() ? userInfo.substring(colonIndex + 1) : "");
            }
        }

        if (removeIfSuccessful) {
            ingestDocument.removeField(field);
        }
        ingestDocument.setFieldValue(targetField, urlParts);
        return ingestDocument;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public UrlPartsProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", "url");
            boolean removeIfSuccessful = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "remove_if_successful", false);
            boolean keepOriginal = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "keep_original", true);
            return new UrlPartsProcessor(processorTag, description, field, targetField, removeIfSuccessful, keepOriginal);
        }
    }
}
