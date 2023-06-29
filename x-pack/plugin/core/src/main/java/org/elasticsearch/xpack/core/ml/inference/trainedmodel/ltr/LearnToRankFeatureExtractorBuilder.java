/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

public interface LearnToRankFeatureExtractorBuilder extends NamedXContentObject, NamedWriteable {

    ParseField FEATURE_NAME = new ParseField("feature_name");

    /**
     * @return The input feature that this extractor satisfies
     */
    String featureName();
}
