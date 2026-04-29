/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.nio.file.Path;

/**
 * Represents a location in a storage system.
 * Uses URI-like format: scheme://[userInfo@]host[:port][/path]
 *
 * Unlike java.net.URI, this class:
 * - Does not perform URL encoding/decoding
 * - Has simpler parsing rules suitable for blob storage keys
 * - Provides convenient methods for path manipulation
 *
 * Note: glob pattern detection ({@link #isPattern()}) only inspects the path component.
 * Glob characters in the scheme or authority (e.g. {@code s3://bucket-*&#47;data/}) are
 * not detected as patterns and will be treated as literal text.
 */
public final class StoragePath {

    public static final String SCHEME_SEPARATOR = "://";
    public static final String PATH_SEPARATOR = "/";
    public static final String PORT_SEPARATOR = ":";

    public static final char[] GLOB_METACHARACTERS = { '*', '?', '{', '[' };

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

    /**
     * Converts a local filesystem {@link Path} to a properly formatted {@code file://} URI string.
     * Normalizes Windows backslashes and ensures the URI has three slashes ({@code file:///}).
     */
    public static String fileUri(Path path) {
        String absPath = path.toAbsolutePath().toString().replace('\\', '/');
        if (absPath.startsWith("/") == false) {
            absPath = "/" + absPath;
        }
        return "file://" + absPath;
    }

    public static StoragePath of(String location) {
        if (location == null) {
            throw new IllegalArgumentException("location cannot be null");
        }

        // Find scheme
        int schemeEnd = location.indexOf(SCHEME_SEPARATOR);
        if (schemeEnd < 0) {
            throw new IllegalArgumentException("Invalid location format, missing scheme: " + location);
        }
        String scheme = location.substring(0, schemeEnd);

        // Parse authority and path
        int authorityStart = schemeEnd + SCHEME_SEPARATOR.length();

        // Handle file:// URIs that may contain Windows drive letters (e.g. file:///C:/path or file://C:\path)
        // Normalize backslashes to forward slashes for file:// URIs before parsing
        String toParse = location;
        if (scheme.equals("file")) {
            toParse = location.substring(0, authorityStart) + location.substring(authorityStart).replace('\\', '/');
        }

        int pathStart = toParse.indexOf('/', authorityStart);
        String authority;
        String path;

        if (pathStart < 0) {
            authority = toParse.substring(authorityStart);
            path = "";
        } else {
            authority = toParse.substring(authorityStart, pathStart);
            path = toParse.substring(pathStart);
        }

        // For file:// URIs with Windows drive letters like file://C:/path,
        // the drive letter ends up as the authority. Fold it back into the path.
        if (scheme.equals("file") && authority.length() == 2 && Character.isLetter(authority.charAt(0)) && authority.charAt(1) == ':') {
            path = "/" + authority + path;
            authority = "";
        }

        // Parse host and port from authority
        String host;
        int port = -1;

        // Skip userInfo if present (not commonly used in storage URLs)
        int atIndex = authority.indexOf('@');
        if (atIndex >= 0) {
            authority = authority.substring(atIndex + 1);
        }

        if (authority.startsWith("[")) {
            // IPv6 literal address per RFC 3986 §3.2.2: [::1] or [::1]:8080
            // The closing bracket delimits the IP literal; port (if any) follows after ']'.
            int closingBracket = authority.indexOf(']');
            if (closingBracket < 0) {
                throw new IllegalArgumentException("Malformed IPv6 address in location (missing ']'): " + location);
            }
            host = authority.substring(0, closingBracket + 1);
            String afterBracket = authority.substring(closingBracket + 1);
            if (afterBracket.isEmpty() == false) {
                if (afterBracket.startsWith(":") == false) {
                    throw new IllegalArgumentException("Malformed authority in location: " + location);
                }
                try {
                    port = Integer.parseInt(afterBracket.substring(1));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid port in location: " + location, e);
                }
            }
        } else {
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

    /**
     * Returns the path component adjusted for use as a local filesystem path.
     * On Windows, file:// URIs produce paths like {@code /C:/dir/file} where the
     * leading slash is invalid for the OS. This method strips it when a drive letter
     * is detected so the result can be passed to {@code PathUtils.get()} safely.
     */
    public String localPath() {
        if (path.length() >= 3 && path.charAt(0) == '/' && Character.isLetter(path.charAt(1)) && path.charAt(2) == ':') {
            return path.substring(1);
        }
        return path;
    }

    public String objectName() {
        if (path.isEmpty() || path.equals(PATH_SEPARATOR)) {
            return "";
        }
        int lastSlash = path.lastIndexOf('/');
        return lastSlash >= 0 ? path.substring(lastSlash + 1) : path;
    }

    public StoragePath parentDirectory() {
        if (path.isEmpty() || path.equals(PATH_SEPARATOR)) {
            return null;
        }
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash <= 0) {
            return StoragePath.of(authorityPrefix() + PATH_SEPARATOR);
        }

        String parentPath = path.substring(0, lastSlash);
        return StoragePath.of(authorityPrefix() + parentPath);
    }

    public StoragePath appendPath(String element) {
        if (element == null) {
            throw new IllegalArgumentException("element cannot be null");
        }
        if (element.isEmpty()) {
            return this;
        }

        String newPath = path;
        boolean pathEndsWithSlash = path.endsWith(PATH_SEPARATOR);
        boolean elementStartsWithSlash = element.startsWith(PATH_SEPARATOR);
        if (pathEndsWithSlash == false && elementStartsWithSlash == false) {
            newPath += PATH_SEPARATOR;
        }
        newPath += element;

        return StoragePath.of(authorityPrefix() + newPath);
    }

    /**
     * Returns true if the path contains glob metacharacters: *, ?, {, [
     */
    public boolean isPattern() {
        for (char c : GLOB_METACHARACTERS) {
            if (path.indexOf(c) >= 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns a new StoragePath truncated to the longest non-pattern prefix directory.
     * e.g. "s3://b/data/2024/*.parquet" -> "s3://b/data/2024/"
     */
    public StoragePath patternPrefix() {
        if (isPattern() == false) {
            return this;
        }
        int firstMeta = firstGlobMetacharacter();
        // Truncate to the last '/' before the first metacharacter
        int lastSlash = path.lastIndexOf('/', firstMeta);
        String prefixPath;
        if (lastSlash < 0) {
            prefixPath = PATH_SEPARATOR;
        } else {
            prefixPath = path.substring(0, lastSlash + 1);
        }

        return StoragePath.of(authorityPrefix() + prefixPath);
    }

    /**
     * Returns the glob portion of the path (everything after the prefix directory).
     * e.g. "s3://b/data/2024/*.parquet" -> "*.parquet"
     */
    public String globPart() {
        if (isPattern() == false) {
            return "";
        }
        int firstMeta = firstGlobMetacharacter();
        // The glob part starts after the last '/' before the first metacharacter
        int lastSlash = path.lastIndexOf('/', firstMeta);
        if (lastSlash < 0) {
            return path;
        }
        return path.substring(lastSlash + 1);
    }

    private String authorityPrefix() {
        String prefix = scheme + SCHEME_SEPARATOR + host;
        if (port > 0) {
            prefix += PORT_SEPARATOR + port;
        }
        return prefix;
    }

    private int firstGlobMetacharacter() {
        int firstMeta = path.length();
        for (char c : GLOB_METACHARACTERS) {
            int idx = path.indexOf(c);
            if (idx >= 0 && idx < firstMeta) {
                firstMeta = idx;
            }
        }
        return firstMeta;
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
