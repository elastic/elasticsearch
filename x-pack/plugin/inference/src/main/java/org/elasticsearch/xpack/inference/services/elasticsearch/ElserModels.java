/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elasticsearch;

import java.util.Set;

public class ElserModels {

    public static final String ELSER_V1_MODEL = ".elser_model_1";
    // Default non platform specific v2 model
    public static final String ELSER_V2_MODEL = ".elser_model_2";
    public static final String ELSER_V2_MODEL_LINUX_X86 = ".elser_model_2_linux-x86_64";

    public static Set<String> VALID_ELSER_MODEL_IDS = Set.of(
        ElserModels.ELSER_V1_MODEL,
        ElserModels.ELSER_V2_MODEL,
        ElserModels.ELSER_V2_MODEL_LINUX_X86
    );

    public static boolean isValidModel(String model) {
        return model != null && VALID_ELSER_MODEL_IDS.contains(model);
    }

    public static boolean isValidEisModel(String model) {
        return ELSER_V2_MODEL.equals(model);
    }

}
