/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteable;

/**
 * Diff that also support NamedWriteable interface
 */
public interface NamedDiff<T extends Diffable<T>> extends Diff<T>, NamedWriteable {
    /**
     * The minimal version of the recipient this object can be sent to.
     * See {@link #supportsVersion(TransportVersion)} for the default serialization check.
     */
    TransportVersion getMinimalSupportedVersion();

    /**
     * Determines whether this instance should be serialized based on the provided transport version.
     *
     * The default implementation returns {@code true} if the given transport version is
     * equal to or newer than {@link #getMinimalSupportedVersion()}.
     * Subclasses may override this method to define custom serialization logic.
     *
     * @param version the transport version of the receiving node
     * @return {@code true} if the instance should be serialized, {@code false} otherwise
     */
    default boolean supportsVersion(TransportVersion version) {
        return version.onOrAfter(getMinimalSupportedVersion());
    }

}
