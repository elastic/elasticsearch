/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearnToRankConfigUpdate;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedInferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedInferenceConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Only the LTR named writeables and xcontent. Remove and combine with inference provider
 * when feature flag is removed
 */
public class MlLTRNamedXContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        // Lenient Inference Config
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LenientlyParsedInferenceConfig.class,
                LearnToRankConfig.NAME,
                LearnToRankConfig::fromXContentLenient
            )
        );
        // Strict Inference Config
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedInferenceConfig.class,
                LearnToRankConfig.NAME,
                LearnToRankConfig::fromXContentStrict
            )
        );
        // Inference Config Update
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                InferenceConfigUpdate.class,
                LearnToRankConfigUpdate.NAME,
                LearnToRankConfigUpdate::fromXContentStrict
            )
        );
        return namedXContent;
    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        // Inference config
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceConfig.class, LearnToRankConfig.NAME.getPreferredName(), LearnToRankConfig::new)
        );
        // Inference config update
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceConfigUpdate.class,
                LearnToRankConfigUpdate.NAME.getPreferredName(),
                LearnToRankConfigUpdate::new
            )
        );
        return namedWriteables;
    }
}
