/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;


public interface InferenceConfigUpdate extends NamedXContentObject, NamedWriteable {

    InferenceConfig apply(InferenceConfig originalConfig);

    InferenceConfig toConfig();

    boolean isSupported(InferenceConfig config);
}
