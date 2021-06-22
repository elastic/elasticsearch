/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.api;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * The intent of this class is to provide an easier way to use URLs.
 */
public class URL {
    
    private final String scheme;
    public String getScheme() {
        return this.scheme;
    }

    private final String host;
    public String getHost() {
        return this.host;
    }

    private int port = -1;
    public int getPort() {
        return this.port;
    }

    private final String path;
    public String getPath() {
        return this.path;
    }

    private final String query;
    public String getQuery() {
        return this.query;
    }

    private final String fragment;
    public String getFragment() {
        return this.fragment;
    }

    /**
     * @param urlString original URL string to construct the instance
     */
    public URL(String urlString) {
        Pattern pattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?([^?#]*)(\\?([^#]*))?(#(.*))?", Pattern.CASE_INSENSITIVE);
        Pattern port = Pattern.compile(".*?(:([0-9]+))?$", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(urlString);

        this.scheme = matcher.group(2);
        Matcher portMatch = port.matcher(matcher.group(4));
        this.host = portMatch.group(1);
        try {
            this.port = Integer.parseInt(portMatch.group(3));
        } catch (NumberFormatException e) {
            this.port = -1;
        }
        this.path = matcher.group(5);
        this.query = matcher.group(7);
        this.fragment = matcher.group(9);
    }

    public boolean hasSameDomain(URL otherURL) {
        return this.scheme == otherURL.scheme && this.host == otherURL.host;
    }
    
    public boolean hasSamePath(URL otherURL) {
        return hasSameDomain(otherURL) && this.path == otherURL.path;
    }

    private static byte[] encode(String url) {
        URL decodedURL = new URL(url);
        String domainPort = decodedURL.host;
        if (decodedURL.port != -1) {
            domainPort = domainPort + ":";
            domainPort = domainPort + String.valueOf(decodedURL.port);
        }
        String encoded = String.format("%s://%s%s/%s", decodedURL.scheme, domainPort, decodedURL.path, decodedURL.query, decodedURL.fragment);
        return encoded.getBytes();
    }
}