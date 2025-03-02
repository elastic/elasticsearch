/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.rerank;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;

public class IbmWatsonxRerankModelTests extends ESTestCase {
    public static IbmWatsonxRerankModel createModel(String model, String projectId, URI uri, String apiVersion, String apiKey) {
        return new IbmWatsonxRerankModel(
            "id",
            TaskType.RERANK,
            "service",
            new IbmWatsonxRerankServiceSettings(uri, apiVersion, model, projectId, null),
            new IbmWatsonxRerankTaskSettings(2, true, 100),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
