/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class UnparsedModelTests extends ESTestCase {

    public void testNullSecrets() {
        UnparsedModel model = new UnparsedModel("id", randomFrom(TaskType.values()), "test_service", Map.of(), null);
        assertThat(model.secrets(), is(nullValue()));
    }

    public void testEmptySecrets_SetToNull() {
        UnparsedModel model = new UnparsedModel("id", randomFrom(TaskType.values()), "test_service", Map.of(), Map.of());
        assertThat(model.secrets(), is(nullValue()));
    }

    public void testSettingsIsModifiable_GivenUnmodifiableMap() {
        UnparsedModel model = new UnparsedModel("id", randomFrom(TaskType.values()), "test_service", Map.of("key", "value"), Map.of());
        model.settings().remove("key");
        assertThat(model.settings().isEmpty(), is(true));
    }

    public void testSecretsIsModifiable_GivenUnmodifiableMap() {
        UnparsedModel model = new UnparsedModel("id", randomFrom(TaskType.values()), "test_service", Map.of(), Map.of("key", "value"));
        model.secrets().remove("key");
        assertThat(model.secrets().isEmpty(), is(true));
    }
}
