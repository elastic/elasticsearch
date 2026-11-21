/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.SecurityContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Checks that the current user may read a stream, the counterpart of {@code AsyncSearchSecurity}.
 * <p>
 * The rule is the one async results use: the stored security headers of the creator are compared with the current authentication
 * through {@link SecurityContext#canIAccessResourcesCreatedWithHeaders}. {@code AsyncSearchSecurity} itself is not reused because
 * its constructor needs an index name, a client and an origin that stream reads do not have.
 */
public class StreamSearchSecurity {

    private final SecurityContext securityContext;

    public StreamSearchSecurity(SecurityContext securityContext) {
        this.securityContext = Objects.requireNonNull(securityContext);
    }

    public boolean currentUserHasAccessToStreamWithHeaders(Map<String, String> headers) throws IOException {
        return securityContext.canIAccessResourcesCreatedWithHeaders(headers);
    }

    /**
     * TODO: the headers checked here are the ones the stream POST stores with {@link StreamStorage#create}. The POST does not store
     * streams yet. It must pass the security headers of the original request (the authentication header), and the real storage must
     * keep them. Empty headers are treated as "created with security off" and allow every caller, so a writer that passes an empty map
     * on a secured cluster would make the stream readable by everyone. Add a check or an assertion on the writer side. The DELETE of a
     * stream, when it exists, needs the same check.
     * <p>
     * Returns {@code metadata} when it exists, has not expired and was created by a user the current user may access.
     * Otherwise throws a {@link ResourceNotFoundException} with the same message in every case, so a caller cannot tell an unknown
     * stream from a stream that belongs to someone else.
     */
    public StreamMetadata getStreamAndCheckAuthentication(String id, @Nullable StreamMetadata metadata, long nowMillis) throws IOException {
        if (metadata == null
            || metadata.expiration().toEpochMilli() < nowMillis
            || currentUserHasAccessToStreamWithHeaders(metadata.headers()) == false) {
            throw new ResourceNotFoundException(id);
        }
        return metadata;
    }
}
