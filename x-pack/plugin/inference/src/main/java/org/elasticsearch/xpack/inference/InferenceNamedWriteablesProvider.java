/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.inference.results.InferenceResult;
import org.elasticsearch.xpack.inference.results.SparseEmbeddingResult;
import org.elasticsearch.xpack.inference.services.elser.ElserMlNodeServiceSettings;
import org.elasticsearch.xpack.inference.services.elser.ElserMlNodeTaskSettings;

import java.util.ArrayList;
import java.util.List;

public class InferenceNamedWriteablesProvider {

    private InferenceNamedWriteablesProvider() {}

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();

        // ELSER config
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, ElserMlNodeServiceSettings.NAME, ElserMlNodeServiceSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(TaskSettings.class, ElserMlNodeTaskSettings.NAME, ElserMlNodeTaskSettings::new)
        );

        // Inference results
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceResult.class, SparseEmbeddingResult.NAME, SparseEmbeddingResult::new)
        );

        return namedWriteables;
    }
}
