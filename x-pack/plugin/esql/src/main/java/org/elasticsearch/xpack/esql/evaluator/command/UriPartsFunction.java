/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.command;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.CompoundOutputFunction;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class UriPartsFunction implements CompoundOutputFunction {
    @Override
    public LinkedHashMap<String, DataType> getOutputColumns() {
        return uriPartsOutputColumns();
    }

    @Override
    public Map<String, Object> evaluate(String uri) {
        return getUriParts(uri);
    }

    // ==================================================================================
    // Logic should be moved to a common library
    // ==================================================================================

    @SuppressForbidden(reason = "URL.getPath is used only if URI.getPath is unavailable")
    private static Map<String, Object> getUriParts(String urlString) {
        URI uri = null;
        URL fallbackUrl = null;
        try {
            uri = new URI(urlString);
        } catch (URISyntaxException e) {
            try {
                // noinspection deprecation
                fallbackUrl = new URL(urlString);
            } catch (MalformedURLException e2) {
                throw new IllegalArgumentException("unable to parse URI [" + urlString + "]");
            }
        }

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
            // To avoid any issues with extracting the extension from a path that contains a dot, we explicitly extract the extension
            // from the last segment in the path.
            var lastSegmentIndex = path.lastIndexOf('/');
            if (lastSegmentIndex >= 0) {
                var lastSegment = path.substring(lastSegmentIndex);
                int periodIndex = lastSegment.lastIndexOf('.');
                if (periodIndex >= 0) {
                    // Don't include the dot in the extension field.
                    uriParts.put("extension", lastSegment.substring(periodIndex + 1));
                }
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
                int colonIndex = userInfo.indexOf(':');
                uriParts.put("username", userInfo.substring(0, colonIndex));
                uriParts.put("password", colonIndex < userInfo.length() ? userInfo.substring(colonIndex + 1) : "");
            }
        }

        return uriParts;
    }

    private static LinkedHashMap<String, DataType> uriPartsOutputColumns() {
        LinkedHashMap<String, DataType> outputColumns = new LinkedHashMap<>();
        outputColumns.put("domain", DataType.KEYWORD);
        outputColumns.put("fragment", DataType.KEYWORD);
        outputColumns.put("path", DataType.KEYWORD);
        outputColumns.put("extension", DataType.KEYWORD);
        outputColumns.put("port", DataType.INTEGER);
        outputColumns.put("query", DataType.KEYWORD);
        outputColumns.put("scheme", DataType.KEYWORD);
        outputColumns.put("user_info", DataType.KEYWORD);
        outputColumns.put("username", DataType.KEYWORD);
        outputColumns.put("password", DataType.KEYWORD);
        return outputColumns;
    }
}
