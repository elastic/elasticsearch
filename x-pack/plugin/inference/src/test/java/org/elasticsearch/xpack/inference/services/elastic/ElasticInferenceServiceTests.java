/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic;

import org.elasticsearch.test.ESTestCase;

public class ElasticInferenceServiceTests extends ESTestCase {

    // TODO: init() (setup mock webserver)

    // TODO: shutdown() (close mock webserver)

    // TODO: test parse request config with valid sparse embeddings model input -> creates new ElasticInferenceServiceSparseEmbeddingsModel

    // TODO: test parse request config with unsupported model -> throws unsupported model type

    // TODO: test parse request config -> throws if extra key exists in overall config

    // TODO: test parse request config -> throws if extra key exists in service settings map

    // TODO: test parse request config -> throws if extra key exists in task settings map (should be empty for now; the task settings, not
    // the test :))

    // TODO: test parse request config -> throws if extra key exists in secret settings map (should be empty for now; the secret settings,
    // not the test :))

    // TODO: test parse persisted config with secrets (we don't have secrets for now (empty secrets), but will have them later, so useful to
    // already test) -> creates sparse embeddings model

    // TODO: test parse persisted config with secrets -> does not throw if extra key exists in overall config

    // TODO: test parse persisted config with secrets -> does not throw if extra key exists in service settings map

    // TODO: test parse persisted config with secrets -> does not throw if extra key exists in task settings map (should be empty for now;
    // the task settings, not the test :))

    // TODO: test parse persisted config with secrets -> does not throw if extra key exists in secret settings map (should be empty for now;
    // the secret settings, not the test :))

    // TODO: test check model config for sparse embeddings model

    // TODO: test infer non-batched mode

    // TODO: test infer batched mode

    // TODO: test infer throw if query is present
}
