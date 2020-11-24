/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.compat;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.MediaType;
import org.elasticsearch.common.xcontent.ParsedMediaType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RestCompatibilityPlugin;
import org.elasticsearch.rest.RestStatus;

public class CompatibleVersionPlugin extends Plugin implements RestCompatibilityPlugin {

    @Override
    public Version getCompatibleVersion(
        @Nullable ParsedMediaType acceptHeader,
        @Nullable ParsedMediaType contentTypeHeader,
        boolean hasContent
    ) {
        Byte aVersion = parseVersion(acceptHeader);
        byte acceptVersion = aVersion == null ? Version.CURRENT.major : Integer.valueOf(aVersion).byteValue();
        Byte cVersion = parseVersion(contentTypeHeader);
        byte contentTypeVersion = cVersion == null ? Version.CURRENT.major : Integer.valueOf(cVersion).byteValue();

        // accept version must be current or prior
        if (acceptVersion > Version.CURRENT.major || acceptVersion < Version.CURRENT.minimumRestCompatibilityVersion().major) {
            throw new ElasticsearchStatusException(
                "Accept version must be either version {} or {}, but found {}. Accept={}",
                RestStatus.BAD_REQUEST,
                Version.CURRENT.major,
                Version.CURRENT.minimumRestCompatibilityVersion().major,
                acceptVersion,
                acceptHeader
            );
        }
        if (hasContent) {

            // content-type version must be current or prior
            if (contentTypeVersion > Version.CURRENT.major
                || contentTypeVersion < Version.CURRENT.minimumRestCompatibilityVersion().major) {
                throw new ElasticsearchStatusException(
                    "Content-Type version must be either version {} or {}, but found {}. Content-Type={}",
                    RestStatus.BAD_REQUEST,
                    Version.CURRENT.major,
                    Version.CURRENT.minimumRestCompatibilityVersion().major,
                    contentTypeVersion,
                    contentTypeHeader
                );
            }
            // if both accept and content-type are sent, the version must match
            if (contentTypeVersion != acceptVersion) {
                throw new ElasticsearchStatusException(
                    "A compatible version is required on both Content-Type and Accept headers "
                        + "if either one has requested a compatible version "
                        + "and the compatible versions must match. Accept={}, Content-Type={}",
                    RestStatus.BAD_REQUEST,
                    acceptHeader,
                    contentTypeHeader
                );
            }
            // both headers should be versioned or none
            if ((cVersion == null && aVersion != null) || (aVersion == null && cVersion != null)) {
                throw new ElasticsearchStatusException(
                    "A compatible version is required on both Content-Type and Accept headers "
                        + "if either one has requested a compatible version. Accept={}, Content-Type={}",
                    RestStatus.BAD_REQUEST,
                    acceptHeader,
                    contentTypeHeader
                );
            }
            if (contentTypeVersion < Version.CURRENT.major) {
                return Version.CURRENT.previousMajor();
            }
        }

        if (acceptVersion < Version.CURRENT.major) {
            return Version.CURRENT.previousMajor();
        }

        return Version.CURRENT;
    }

    // scope for testing
    static Byte parseVersion(ParsedMediaType parsedMediaType) {
        if (parsedMediaType != null) {
            String version = parsedMediaType.getParameters().get(MediaType.COMPATIBLE_WITH_PARAMETER_NAME);
            return version != null ? Byte.parseByte(version) : null;
        }
        return null;
    }
}
