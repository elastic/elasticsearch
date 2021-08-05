/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

public interface TrainedModelLocation extends NamedXContentObject, NamedWriteable {

    String getModelId();

    String getResourceName();

    default Version getMinimalCompatibilityVersion() {
        return TrainedModelConfig.VERSION_3RD_PARTY_CONFIG_ADDED;
    }
}
