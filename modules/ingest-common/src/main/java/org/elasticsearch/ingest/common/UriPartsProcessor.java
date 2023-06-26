/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
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
        URI uri = null;
        URL url = null;
        try {
            uri = new URI(urlString);
        } catch (URISyntaxException e) {
            try {
                url = new URL(urlString);
            } catch (MalformedURLException e2) {
                throw new IllegalArgumentException("unable to parse URI [" + urlString + "]");
            }
        }
        return getUriParts(uri, url);
    }

    @SuppressForbidden(reason = "URL.getPath is used only if URI.getPath is unavailable")
    private static Map<String, Object> getUriParts(URI uri, URL fallbackUrl) {
        var uriParts = new HashMap<String, Object>();
        String domain;
        String fragment;
        String path;
        int port;
        String query;
        String scheme;
        String userInfo;

        if (uri != null) {
            domain = uri.getHost();
            fragment = uri.getFragment();
            path = uri.getPath();
            port = uri.getPort();
            query = uri.getQuery();
            scheme = uri.getScheme();
            userInfo = uri.getUserInfo();
        } else if (fallbackUrl != null) {
            domain = fallbackUrl.getHost();
            fragment = fallbackUrl.getRef();
            path = fallbackUrl.getPath();
            port = fallbackUrl.getPort();
            query = fallbackUrl.getQuery();
            scheme = fallbackUrl.getProtocol();
            userInfo = fallbackUrl.getUserInfo();
        } else {
            // should never occur during processor execution
            throw new IllegalArgumentException("at least one argument must be non-null");
        }

        uriParts.put("domain", domain);
        if (fragment != null) {
            uriParts.put("fragment", fragment);
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
        uriParts.put("scheme", scheme);
        if (userInfo != null) {
            uriParts.put("user_info", userInfo);
            if (userInfo.contains(":")) {
                int colonIndex = userInfo.indexOf(":");
                uriParts.put("username", userInfo.substring(0, colonIndex));
                uriParts.put("password", colonIndex < userInfo.length() ? userInfo.substring(colonIndex + 1) : "");
            }
        }

        return uriParts;
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
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            return new UriPartsProcessor(processorTag, description, field, targetField, removeIfSuccessful, keepOriginal, ignoreMissing);
        }
    }
}
