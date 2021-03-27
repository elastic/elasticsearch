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

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.MalformedURLException;
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
        URL url;
        Strign scheme = null;
        String domain = null;
        String fragment = null;
        String path = null;
        Integer port = -1;
        String query = null;
        String userInfo = null;
        try {
            uri = new URI(value);
            scheme = uri.getScheme();
            domain = uri.getHost();
            fragment = uri.getFragment();
            path = uri.getPath();
            port = uri.getPort();
            query = uri.getQuery();
            userInfo = uri.getUserInfo();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("unable to parse URI [" + value + "]");
        }
        try {
            url = new URL(value);
            scheme = url.getScheme();
            domain = url.getHost();
            // fragment = url.getFragment();
            path = url.getPath();
            port = url.getPort();
            query = url.getQuery();
            userInfo = url.getUserInfo();
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("unable to parse URL [" + value + "]");
        }
        var uriParts = new HashMap<String, Object>();
        if (domain != null) {
            uriParts.put("domain", domain);
        }
        if (fragment != null) {
            uriParts.put("fragment", fragment);
        } else {
            if (value.contains("#")) {
                int hashIndex = value.lastIndexOf('#');
                fragment = hashIndex < value.length() ? value.substring(hashIndex + 1) : "";
            }
        }
        if (keepOriginal) {
            uriParts.put("original", value);
        }
        if (path != null) {
            uriParts.put("path", path);
            if (path.contains(".")) {
                int periodIndex = path.lastIndexOf('.');
                uriParts.put("extension", periodIndex < path.length() ? path.substring(periodIndex + 1) : "");
            }
        }
        if (port != -1) {
            uriParts.put("port", port);
        }
        if (query != null) {
            uriParts.put("query", query);
        }
        if (scheme != null) {
            uriParts.put("scheme", scheme);
        }
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
