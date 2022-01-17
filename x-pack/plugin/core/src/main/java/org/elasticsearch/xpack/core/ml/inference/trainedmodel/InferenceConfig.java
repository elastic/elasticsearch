/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

public interface InferenceConfig extends NamedXContentObject, NamedWriteable {

    String DEFAULT_TOP_CLASSES_RESULTS_FIELD = "top_classes";
    String DEFAULT_RESULTS_FIELD = "predicted_value";

    boolean isTargetTypeSupported(TargetType targetType);

    /**
     * All nodes in the cluster must be at least this version
     */
    Version getMinimalSupportedVersion();

    default boolean requestingImportance() {
        return false;
    }

    String getResultsField();

    boolean isAllocateOnly();
}
