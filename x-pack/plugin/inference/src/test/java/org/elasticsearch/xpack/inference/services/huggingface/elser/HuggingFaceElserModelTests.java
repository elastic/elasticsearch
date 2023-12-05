/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.elser;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class HuggingFaceElserModelTests extends ESTestCase {

    public void testThrowsURISyntaxException_ForInvalidUrl() {
        var thrownException = expectThrows(IllegalArgumentException.class, () -> createModel("^^", "secret"));
        assertThat(thrownException.getMessage(), is("unable to parse url [^^]"));
    }

    public static HuggingFaceElserModel createModel(String url, String apiKey) {
        return new HuggingFaceElserModel(
            "id",
            TaskType.SPARSE_EMBEDDING,
            "service",
            new HuggingFaceElserServiceSettings(url),
            new HuggingFaceElserSecretSettings(new SecureString(apiKey.toCharArray()))
        );
    }
}
