/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

public interface TrainedModel extends NamedXContentObject, NamedWriteable, Accountable {

    /**
     * @return {@link TargetType} for the model.
     */
    TargetType targetType();

    /**
     * Runs validations against the model.
     *
     * Example: {@link org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.Tree} should check if there are any loops
     *
     * @throws org.elasticsearch.ElasticsearchException if validations fail
     */
    void validate();

    /**
     * @return The estimated number of operations required at inference time
     */
    long estimatedNumOperations();

    default TransportVersion getMinimalCompatibilityVersion() {
        return TransportVersion.V_7_6_0;
    }
}
