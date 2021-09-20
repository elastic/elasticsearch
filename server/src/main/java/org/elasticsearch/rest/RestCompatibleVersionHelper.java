/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.rest;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.common.xcontent.MediaType;
import org.elasticsearch.common.xcontent.ParsedMediaType;

/**
 * A helper that is responsible for parsing a Compatible REST API version from RestRequest.
 * It also performs a validation of allowed combination of versions provided on those headers.
 * Package scope as it is only aimed to be used by RestRequest
 */
class RestCompatibleVersionHelper {

    static RestApiVersion getCompatibleVersion(
        @Nullable ParsedMediaType acceptHeader,
        @Nullable ParsedMediaType contentTypeHeader,
        boolean hasContent
    ) {
        Byte aVersion = parseVersion(acceptHeader);
        byte acceptVersion = aVersion == null ? RestApiVersion.current().major : Integer.valueOf(aVersion).byteValue();
        Byte cVersion = parseVersion(contentTypeHeader);
        byte contentTypeVersion = cVersion == null ?
            RestApiVersion.current().major : Integer.valueOf(cVersion).byteValue();

        // accept version must be current or prior
        if (acceptVersion > RestApiVersion.current().major ||
            acceptVersion < RestApiVersion.minimumSupported().major) {
            throw new ElasticsearchStatusException(
                "Accept version must be either version {} or {}, but found {}. Accept={}",
                RestStatus.BAD_REQUEST,
                RestApiVersion.current().major,
                RestApiVersion.minimumSupported().major,
                acceptVersion,
                acceptHeader
            );
        }
        if (hasContent) {

            // content-type version must be current or prior
            if (contentTypeVersion > RestApiVersion.current().major
                || contentTypeVersion < RestApiVersion.minimumSupported().major) {
                throw new ElasticsearchStatusException(
                    "Content-Type version must be either version {} or {}, but found {}. Content-Type={}",
                    RestStatus.BAD_REQUEST,
                    RestApiVersion.current().major,
                    RestApiVersion.minimumSupported().major,
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
            if (contentTypeVersion < RestApiVersion.current().major) {
                return RestApiVersion.minimumSupported();
            }
        }

        if (acceptVersion < RestApiVersion.current().major) {
            return RestApiVersion.minimumSupported();
        }

        return RestApiVersion.current();
    }

    static Byte parseVersion(ParsedMediaType parsedMediaType) {
        if (parsedMediaType != null) {
            String version = parsedMediaType.getParameters().get(MediaType.COMPATIBLE_WITH_PARAMETER_NAME);
            return version != null ? Byte.parseByte(version) : null;
        }
        return null;
    }
}
