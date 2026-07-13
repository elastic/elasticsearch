/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq;

import org.elasticsearch.inference.InferenceServiceConfiguration;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;

import java.util.EnumSet;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;

public class GroqServiceConfigurationTests extends ESTestCase {

    public void testConfigurationMetadata() {
        InferenceServiceConfiguration configuration = GroqService.Configuration.get();
        assertThat(configuration.getService(), equalTo(GroqService.NAME));
        assertThat(configuration.getName(), containsString("Groq"));
        assertThat(configuration.getTaskTypes(), equalTo(EnumSet.of(TaskType.CHAT_COMPLETION)));
        assertThat(configuration.getConfigurations().keySet(), hasItems("model_id", "url", "api_key"));
    }
}
