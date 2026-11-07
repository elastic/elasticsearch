/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.pin;

import java.io.IOException;
import java.util.List;

/**
 * Metadata-only access to a remote store, for pin capture and verification. Implementations must
 * never fetch object bodies: pins exist to detect upstream drift, and drift detection over
 * multi-GiB public objects has to stay a metadata operation.
 */
public interface PinProbe {

    /** Metadata of a single object, via HTTP {@code HEAD} or equivalent. */
    ObjectMetadata head(String uri) throws IOException;

    /**
     * Metadata of the objects under a prefix (glob metacharacters stripped by the caller), via S3
     * {@code ListObjectsV2} or equivalent. Providers that cannot list (plain HTTPS) must throw
     * {@link UnsupportedOperationException} — the "HTTP cannot list" constraint expressed in code.
     *
     * @param uri     the prefix URI
     * @param maxKeys upper bound on returned entries
     */
    List<ObjectMetadata> list(String uri, int maxKeys) throws IOException;
}
