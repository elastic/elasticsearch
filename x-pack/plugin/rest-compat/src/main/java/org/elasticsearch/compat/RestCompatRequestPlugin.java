/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.compat;

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RestRequestPlugin;
import org.elasticsearch.rest.CompatibleConstants;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFactory;

import java.util.Locale;

public class RestCompatRequestPlugin extends Plugin implements RestRequestPlugin {

    @Override
    public RestRequestFactory getRestRequestFactory() {
        return new RestRequestFactory() {
            @Override
            public RestRequest createRestRequest(RestRequest restRequest) {
                return new CompatibleRestRequest(restRequest);
            }
        };
    }

    public static class CompatibleRestRequest extends RestRequest {

        // TODO this requires copying the content of original rest request. Isn't this against why multiple rest wrappers were disallowed?
        protected CompatibleRestRequest(RestRequest restRequest) {
            super(restRequest);
        }

        @Override
        public Version getCompatibleApiVersion() {
            if (isRequestingCompatibility()) {
                return Version.minimumRestCompatibilityVersion();
            } else {
                return Version.CURRENT;
            }
        }

        private boolean isRequestingCompatibility() {
            String acceptHeader = header(CompatibleConstants.COMPATIBLE_ACCEPT_HEADER);
            String aVersion = XContentType.parseVersion(acceptHeader);
            byte acceptVersion = aVersion == null ? Version.CURRENT.major : Integer.valueOf(aVersion).byteValue();
            String contentTypeHeader = header(CompatibleConstants.COMPATIBLE_CONTENT_TYPE_HEADER);
            String cVersion = XContentType.parseVersion(contentTypeHeader);
            byte contentTypeVersion = cVersion == null ? Version.CURRENT.major : Integer.valueOf(cVersion).byteValue();

            if (Version.CURRENT.major < acceptVersion || Version.CURRENT.major - acceptVersion > 1) {
                throw new CompatibleApiHeadersCombinationException(
                    String.format(
                        Locale.ROOT,
                        "Unsupported version provided. " + "Accept=%s Content-Type=%s hasContent=%b path=%s params=%s method=%s",
                        acceptHeader,
                        contentTypeHeader,
                        hasContent(),
                        path(),
                        params().toString(),
                        method().toString()
                    )
                );
            }
            if (hasContent()) {
                if (Version.CURRENT.major < contentTypeVersion || Version.CURRENT.major - contentTypeVersion > 1) {
                    throw new CompatibleApiHeadersCombinationException(
                        String.format(
                            Locale.ROOT,
                            "Unsupported version provided. " + "Accept=%s Content-Type=%s hasContent=%b path=%s params=%s method=%s",
                            acceptHeader,
                            contentTypeHeader,
                            hasContent(),
                            path(),
                            params().toString(),
                            method().toString()
                        )
                    );
                }

                if (contentTypeVersion != acceptVersion) {
                    throw new CompatibleApiHeadersCombinationException(
                        String.format(
                            Locale.ROOT,
                            "Content-Type and Accept headers have to match when content is present. "
                                + "Accept=%s Content-Type=%s hasContent=%b path=%s params=%s method=%s",
                            acceptHeader,
                            contentTypeHeader,
                            hasContent(),
                            path(),
                            params().toString(),
                            method().toString()
                        )
                    );
                }
                // both headers should be versioned or none
                if ((cVersion == null && aVersion != null) || (aVersion == null && cVersion != null)) {
                    throw new RestRequest.CompatibleApiHeadersCombinationException(
                        String.format(
                            Locale.ROOT,
                            "Versioning is required on both Content-Type and Accept headers. "
                                + "Accept=%s Content-Type=%s hasContent=%b path=%s params=%s method=%s",
                            acceptHeader,
                            contentTypeHeader,
                            hasContent(),
                            path(),
                            params().toString(),
                            method().toString()
                        )
                    );
                }

                return contentTypeVersion < Version.CURRENT.major;
            }

            return acceptVersion < Version.CURRENT.major;
        }

    }
}
