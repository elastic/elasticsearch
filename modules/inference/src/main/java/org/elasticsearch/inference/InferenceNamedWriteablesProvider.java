/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.inference.results.InferenceResult;
import org.elasticsearch.inference.results.SparseEmbeddingResult;
import org.elasticsearch.inference.services.elser.ElserServiceSettings;
import org.elasticsearch.inference.services.elser.ElserSparseEmbeddingTaskSettings;

import java.util.ArrayList;
import java.util.List;

public class InferenceNamedWriteablesProvider {

    private InferenceNamedWriteablesProvider() {}

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();

        // ELSER config
        namedWriteables.add(new NamedWriteableRegistry.Entry(ServiceSettings.class, ElserServiceSettings.NAME, ElserServiceSettings::new));
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                TaskSettings.class,
                ElserSparseEmbeddingTaskSettings.NAME,
                ElserSparseEmbeddingTaskSettings::new
            )
        );

        // Inference results
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceResult.class, SparseEmbeddingResult.NAME, SparseEmbeddingResult::new)
        );

        return namedWriteables;
    }
}
