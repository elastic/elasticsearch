/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.compat;

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RestCompatibility;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CompatRestRequest extends Plugin implements RestCompatibility {

    private static final Pattern COMPATIBLE_API_HEADER_PATTERN = Pattern.compile(
        "(application|text)/(vnd.elasticsearch\\+)?([^;]+)(\\s*;\\s*compatible-with=(\\d+))?",
        Pattern.CASE_INSENSITIVE);

    @Override
    public Version getCompatibleVersion(@Nullable String acceptHeader, @Nullable String contentTypeHeader) {

        Integer acceptVersion = acceptHeader == null ? null : parseVersion(acceptHeader);
        Integer contentTypeVersion = contentTypeHeader == null ? null : parseVersion(contentTypeHeader);

        //request version must be current or prior
        if (acceptVersion != null && acceptVersion > Version.CURRENT.major ||
            contentTypeVersion != null && contentTypeVersion > Version.CURRENT.major) {
            throw new CompatibleApiException(
                String.format(Locale.ROOT, "Compatible version must be equal or less then the current version. " +
                    "Accept=%s Content-Type=%s", acceptHeader, contentTypeHeader));
        }

        //request version can not be older then last major
        if (acceptVersion != null && acceptVersion < Version.CURRENT.major - 1 ||
            contentTypeVersion != null && contentTypeVersion < Version.CURRENT.major - 1) {
            throw new CompatibleApiException(
                String.format(Locale.ROOT, "Compatible versioning only is only available for past major version. " +
                    "Accept=%s Content-Type=%s", acceptHeader, contentTypeHeader));
        }

        // if a compatible content type is sent, so must a versioned accept header.
        if (contentTypeVersion != null && acceptVersion == null ) {
            throw new CompatibleApiException(
                String.format(Locale.ROOT, "The Accept header must have request a version if the Content-Type version is requested." +
                    "Accept=%s Content-Type=%s", acceptHeader, contentTypeHeader));
        }

        // if both accept and content-type are sent , the version must match
        if (acceptVersion != null && contentTypeVersion != null && contentTypeVersion != acceptVersion) {
            throw new CompatibleApiException(
                String.format(Locale.ROOT, "Content-Type and Accept version requests have to match. " +
                        "Accept=%s Content-Type=%s", acceptHeader,
                    contentTypeHeader));
        }
        return Version.fromString(Version.CURRENT.major - 1 + ".0.0");
    }

    private static Integer parseVersion(String mediaType) {
        if (mediaType != null) {
            Matcher matcher = COMPATIBLE_API_HEADER_PATTERN.matcher(mediaType);
            if (matcher.find() && "vnd.elasticsearch+".equalsIgnoreCase(matcher.group(2))) {
                return Integer.valueOf(matcher.group(5));
            }
        }
        return null;
    }
}
