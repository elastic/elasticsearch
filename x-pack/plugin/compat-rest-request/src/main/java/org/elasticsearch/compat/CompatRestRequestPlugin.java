/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.compat;

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RestCompatibilityPlugin;

import java.util.List;
import java.util.Locale;
import java.util.Map;

public class CompatRestRequestPlugin extends Plugin implements RestCompatibilityPlugin {
    private static final String COMPATIBLE_ACCEPT_HEADER = "Accept";
    private static final String COMPATIBLE_CONTENT_TYPE_HEADER = "Content-Type";

    @Override
    public boolean isRequestingCompatibility(Map<String, List<String>>  headers, boolean hasContent) {
        String acceptHeader = header(headers, COMPATIBLE_ACCEPT_HEADER);
        String aVersion = XContentType.parseVersion(acceptHeader);
        byte acceptVersion = aVersion == null ? Version.CURRENT.major : Integer.valueOf(aVersion).byteValue();
        String contentTypeHeader = header(headers, COMPATIBLE_CONTENT_TYPE_HEADER);
        String cVersion = XContentType.parseVersion(contentTypeHeader);
        byte contentTypeVersion = cVersion == null ? Version.CURRENT.major : Integer.valueOf(cVersion).byteValue();

        if(Version.CURRENT.major < acceptVersion || Version.CURRENT.major - acceptVersion > 1 ){
            throw new CompatibleApiHeadersCombinationException(
                String.format(Locale.ROOT, "Unsupported version provided. " +
                        "Accept=%s Content-Type=%s hasContent=%b", acceptHeader,
                    contentTypeHeader, hasContent));
        }
        if (hasContent) {
            if(Version.CURRENT.major < contentTypeVersion || Version.CURRENT.major - contentTypeVersion > 1 ){
                throw new CompatibleApiHeadersCombinationException(
                    String.format(Locale.ROOT, "Unsupported version provided. " +
                            "Accept=%s Content-Type=%s hasContent=%b", acceptHeader,
                        contentTypeHeader, hasContent));
            }

            if (contentTypeVersion != acceptVersion) {
                throw new CompatibleApiHeadersCombinationException(
                    String.format(Locale.ROOT, "Content-Type and Accept headers have to match when content is present. " +
                            "Accept=%s Content-Type=%s hasContent=%b", acceptHeader,
                        contentTypeHeader, hasContent));
            }
            // both headers should be versioned or none
            if ((cVersion == null && aVersion!=null) || (aVersion ==null && cVersion!=null) ){
                throw new CompatibleApiHeadersCombinationException(
                    String.format(Locale.ROOT, "Versioning is required on both Content-Type and Accept headers. " +
                            "Accept=%s Content-Type=%s hasContent=%b path=%s params=%s method=%s", acceptHeader,
                        contentTypeHeader, hasContent));
            }

            return contentTypeVersion < Version.CURRENT.major;
        }

        return acceptVersion < Version.CURRENT.major;
    }

    public final String header(Map<String, List<String>>  headers, String name) {
        List<String> values = headers.get(name);
        if (values != null && values.isEmpty() == false) {
            return values.get(0);
        }
        return null;
    }


    public static class CompatibleApiHeadersCombinationException extends RuntimeException {

        CompatibleApiHeadersCombinationException(String cause) {
            super(cause);
        }
    }
}
