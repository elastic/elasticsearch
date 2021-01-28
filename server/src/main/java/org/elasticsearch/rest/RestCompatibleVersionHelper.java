/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.rest;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.MediaType;
import org.elasticsearch.common.xcontent.ParsedMediaType;

/**
 * A helper that is responsible for parsing a Compatible REST API version from RestRequest.
 * It also performs a validation of allowed combination of versions provided on those headers.
 * Package scope as it is only aimed to be used by RestRequest
 */
class RestCompatibleVersionHelper {

    static Version getCompatibleVersion(
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

    static Byte parseVersion(ParsedMediaType parsedMediaType) {
        if (parsedMediaType != null) {
            String version = parsedMediaType.getParameters().get(MediaType.COMPATIBLE_WITH_PARAMETER_NAME);
            return version != null ? Byte.parseByte(version) : null;
        }
        return null;
    }
}
