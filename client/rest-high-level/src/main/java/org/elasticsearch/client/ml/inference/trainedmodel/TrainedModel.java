/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.inference.trainedmodel;

import org.elasticsearch.client.ml.inference.NamedXContentObject;

import java.util.List;

public interface TrainedModel extends NamedXContentObject {

    /**
     * @return List of featureNames expected by the model. In the order that they are expected
     */
    List<String> getFeatureNames();

    /**
     * @return The name of the model
     */
    String getName();
}
