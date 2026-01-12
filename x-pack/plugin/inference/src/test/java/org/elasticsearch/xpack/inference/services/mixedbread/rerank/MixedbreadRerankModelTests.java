/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.rerank;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

public class MixedbreadRerankModelTests {
    public static MixedbreadRerankModel createModel(String model, String apiKey) {
        return new MixedbreadRerankModel(
            model,
            new MixedbreadRerankServiceSettings(model, null),
            new MixedbreadRerankTaskSettings(null, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
