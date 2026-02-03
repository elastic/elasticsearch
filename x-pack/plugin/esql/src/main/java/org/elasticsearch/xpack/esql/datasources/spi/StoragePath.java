/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Represents a location in a storage system. Similar to Trino's Location class.
 * Uses URI-like format: scheme://[userInfo@]host[:port][/path]
 *
 * Unlike java.net.URI, this class:
 * - Does not perform URL encoding/decoding
 * - Has simpler parsing rules suitable for blob storage keys
 * - Provides convenient methods for path manipulation
 */
public final class StoragePath {
    private final String location;
    private final String scheme;      // "s3", "https", "file", etc.
    private final String host;        // bucket name, hostname
    private final int port;           // -1 if not specified
    private final String path;        // path within the storage

    private StoragePath(String location, String scheme, String host, int port, String path) {
        this.location = location;
        this.scheme = scheme;
        this.host = host;
        this.port = port;
        this.path = path;
    }

    public static StoragePath of(String location) {
        if (location == null) {
            throw new IllegalArgumentException("location cannot be null");
        }

        // Find scheme
        int schemeEnd = location.indexOf("://");
        if (schemeEnd < 0) {
            throw new IllegalArgumentException("Invalid location format, missing scheme: " + location);
        }
        String scheme = location.substring(0, schemeEnd);

        // Parse authority and path
        int pathStart = location.indexOf('/', schemeEnd + 3);
        String authority;
        String path;

        if (pathStart < 0) {
            authority = location.substring(schemeEnd + 3);
            path = "";
        } else {
            authority = location.substring(schemeEnd + 3, pathStart);
            path = location.substring(pathStart);
        }

        // Parse host and port from authority
        String host;
        int port = -1;

        // Skip userInfo if present (not commonly used in storage URLs)
        int atIndex = authority.indexOf('@');
        if (atIndex >= 0) {
            authority = authority.substring(atIndex + 1);
        }

        int portIndex = authority.lastIndexOf(':');
        if (portIndex >= 0) {
            host = authority.substring(0, portIndex);
            try {
                port = Integer.parseInt(authority.substring(portIndex + 1));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid port in location: " + location, e);
            }
        } else {
            host = authority;
        }

        return new StoragePath(location, scheme, host, port, path);
    }

    public String scheme() {
        return scheme;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public String path() {
        return path;
    }

    public String objectName() {
        if (path.isEmpty() || path.equals("/")) {
            return "";
        }
        int lastSlash = path.lastIndexOf('/');
        return lastSlash >= 0 ? path.substring(lastSlash + 1) : path;
    }

    public StoragePath parentDirectory() {
        if (path.isEmpty() || path.equals("/")) {
            return null;
        }
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash <= 0) {
            // Root level
            String parentLocation = scheme + "://" + host;
            if (port > 0) {
                parentLocation += ":" + port;
            }
            parentLocation += "/";
            return StoragePath.of(parentLocation);
        }

        String parentPath = path.substring(0, lastSlash);
        String parentLocation = scheme + "://" + host;
        if (port > 0) {
            parentLocation += ":" + port;
        }
        parentLocation += parentPath;
        return StoragePath.of(parentLocation);
    }

    public StoragePath appendPath(String element) {
        if (element == null) {
            throw new IllegalArgumentException("element cannot be null");
        }
        if (element.isEmpty()) {
            return this;
        }

        String newPath = path;
        boolean pathEndsWithSlash = path.endsWith("/");
        boolean elementStartsWithSlash = element.startsWith("/");
        if (pathEndsWithSlash == false && elementStartsWithSlash == false) {
            newPath += "/";
        }
        newPath += element;

        String newLocation = scheme + "://" + host;
        if (port > 0) {
            newLocation += ":" + port;
        }
        newLocation += newPath;

        return StoragePath.of(newLocation);
    }

    @Override
    public String toString() {
        return location;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StoragePath that = (StoragePath) o;
        return location.equals(that.location);
    }

    @Override
    public int hashCode() {
        return location.hashCode();
    }
}
