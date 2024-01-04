/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.ltr;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LearningToRankConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedInferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedInferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.LearningToRankFeatureExtractorBuilder;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ltr.QueryExtractorBuilder;

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
                LearningToRankConfig.NAME,
                LearningToRankConfig::fromXContentLenient
            )
        );
        // Strict Inference Config
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                StrictlyParsedInferenceConfig.class,
                LearningToRankConfig.NAME,
                LearningToRankConfig::fromXContentStrict
            )
        );
        // LTR extractors
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                LearningToRankFeatureExtractorBuilder.class,
                QueryExtractorBuilder.NAME,
                QueryExtractorBuilder::fromXContent
            )
        );
        return namedXContent;
    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        // Inference config
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceConfig.class, LearningToRankConfig.NAME.getPreferredName(), LearningToRankConfig::new)
        );
        // LTR Extractors
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                LearningToRankFeatureExtractorBuilder.class,
                QueryExtractorBuilder.NAME.getPreferredName(),
                QueryExtractorBuilder::new
            )
        );
        return namedWriteables;
    }
}
