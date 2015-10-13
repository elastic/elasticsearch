/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.support;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;

import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public final class VersionUtils {

    public static final String VERSION_NUMBER_FIELD = "number";

    private VersionUtils() {
    }

    public static Version parseVersion(byte[] text) {
        return parseVersion(VERSION_NUMBER_FIELD, new String(text, Charset.forName("UTF-8")));
    }

    /**
     * Extract &amp; parse the version contained in the given template
     */
    public static Version parseVersion(String prefix, byte[] text) {
        return parseVersion(prefix, new String(text, Charset.forName("UTF-8")));
    }

    public static Version parseVersion(String prefix, String text) {
        Pattern pattern = Pattern.compile(prefix + "\"\\s*:\\s*\"?([0-9a-zA-Z\\.\\-]+)\"?");
        Matcher matcher = pattern.matcher(text);
        if (matcher.find()) {
            String parsedVersion = matcher.group(1);
            if (Strings.hasText(parsedVersion)) {
                return Version.fromString(parsedVersion);
            }
        }
        return null;
    }
}
