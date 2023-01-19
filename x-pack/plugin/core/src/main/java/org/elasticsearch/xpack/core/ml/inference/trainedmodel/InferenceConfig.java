/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

public interface InferenceConfig extends NamedXContentObject, VersionedNamedWriteable {

    String DEFAULT_TOP_CLASSES_RESULTS_FIELD = "top_classes";
    String DEFAULT_RESULTS_FIELD = "predicted_value";

    boolean isTargetTypeSupported(TargetType targetType);

    @Override
    default TransportVersion getMinimalSupportedVersion() {
        /*
         * TODO: This method existed before it inherited from VersionedNamedWriteable,
         *  so we need to look at this closely when we migrate the bulk of the ML code,
         *  whether this actually means the transport version or the actual node version
         *  on the other end
         */
        return getMinimalSupportedNodeVersion().transportVersion;
    }

    /**
     * All nodes in the cluster must be at least this version
     */
    Version getMinimalSupportedNodeVersion();

    default boolean requestingImportance() {
        return false;
    }

    String getResultsField();

    boolean isAllocateOnly();
}
