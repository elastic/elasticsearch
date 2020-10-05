/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.compat;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RestCompatibilityPlugin;
import org.elasticsearch.rest.RestStatus;

public class CompatibleVersionPlugin extends Plugin implements RestCompatibilityPlugin {

    @Override
    public Version getCompatibleVersion(@Nullable String acceptHeader, @Nullable String contentTypeHeader, boolean hasContent) {
        Byte aVersion = XContentType.parseVersion(acceptHeader);
        byte acceptVersion = aVersion == null ? Version.CURRENT.major : Integer.valueOf(aVersion).byteValue();
        Byte cVersion = XContentType.parseVersion(contentTypeHeader);
        byte contentTypeVersion = cVersion == null ? Version.CURRENT.major : Integer.valueOf(cVersion).byteValue();

        // accept version must be current or prior
        if (acceptVersion > Version.CURRENT.major || acceptVersion < Version.CURRENT.major - 1) {
            throw new ElasticsearchStatusException(
                "Compatible version must be equal or less then the current version. Accept={}} Content-Type={}}",
                RestStatus.BAD_REQUEST,
                acceptHeader,
                contentTypeHeader
            );
        }
        if (hasContent) {

            // content-type version must be current or prior
            if (contentTypeVersion > Version.CURRENT.major || contentTypeVersion < Version.CURRENT.major - 1) {
                throw new ElasticsearchStatusException(
                    "Compatible version must be equal or less then the current version. Accept={} Content-Type={}",
                    RestStatus.BAD_REQUEST,
                    acceptHeader,
                    contentTypeHeader,
                    RestStatus.BAD_REQUEST
                );
            }
            // if both accept and content-type are sent, the version must match
            if (contentTypeVersion != acceptVersion) {
                throw new ElasticsearchStatusException(
                    "Content-Type and Accept version requests have to match. Accept={} Content-Type={}",
                    RestStatus.BAD_REQUEST,
                    acceptHeader,
                    contentTypeHeader
                );
            }
            // both headers should be versioned or none
            if ((cVersion == null && aVersion != null) || (aVersion == null && cVersion != null)) {
                throw new ElasticsearchStatusException(
                    "Versioning is required on both Content-Type and Accept headers. Accept={} Content-Type={}",
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
}
