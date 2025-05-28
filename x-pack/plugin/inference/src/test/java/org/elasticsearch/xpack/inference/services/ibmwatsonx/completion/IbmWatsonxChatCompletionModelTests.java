/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.completion;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.net.URI;
import java.net.URISyntaxException;

public class IbmWatsonxChatCompletionModelTests extends ESTestCase {

    public static IbmWatsonxChatCompletionModel createModel(URI uri, String apiVersion, String modelId, String projectId, String apiKey)
        throws URISyntaxException {
        return new IbmWatsonxChatCompletionModel(
            "id",
            TaskType.COMPLETION,
            "service",
            new IbmWatsonxChatCompletionServiceSettings(uri, apiVersion, modelId, projectId, null),
            new DefaultSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
