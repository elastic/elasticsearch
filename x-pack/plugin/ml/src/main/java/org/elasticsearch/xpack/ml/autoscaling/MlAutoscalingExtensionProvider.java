/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.xpack.autoscaling.AutoscalingExtensionProvider;
import org.elasticsearch.xpack.ml.MachineLearning;

/** ML autoscaling provider for painless extensions. */
public class MlAutoscalingExtensionProvider extends AutoscalingExtensionProvider {

    public MlAutoscalingExtensionProvider() {  // explicit no-args ctr, for SL
        super(MlAutoscalingExtension.class,    // Autoscaling extension type
              MachineLearning.class);          // ctr arg type
    }
}
