/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.ml.AbstractBWCSerializationTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.FillMaskConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NerConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NerConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.NlpConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PassThroughConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.PassThroughConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.QuestionAnsweringConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.QuestionAnsweringConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextClassificationConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextEmbeddingConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextExpansionConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextSimilarityConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TextSimilarityConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ZeroShotClassificationConfigTests;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase.getAllBWCVersions;

public abstract class InferenceConfigItemTestCase<T extends VersionedNamedWriteable & ToXContent> extends AbstractBWCSerializationTestCase<
    T> {

    static InferenceConfig mutateForVersion(NlpConfig inferenceConfig, TransportVersion version) {
        if (inferenceConfig instanceof TextClassificationConfig textClassificationConfig) {
            return TextClassificationConfigTests.mutateForVersion(textClassificationConfig, version);
        } else if (inferenceConfig instanceof FillMaskConfig fillMaskConfig) {
            return FillMaskConfigTests.mutateForVersion(fillMaskConfig, version);
        } else if (inferenceConfig instanceof QuestionAnsweringConfig questionAnsweringConfig) {
            return QuestionAnsweringConfigTests.mutateForVersion(questionAnsweringConfig, version);
        } else if (inferenceConfig instanceof NerConfig nerConfig) {
            return NerConfigTests.mutateForVersion(nerConfig, version);
        } else if (inferenceConfig instanceof PassThroughConfig passThroughConfig) {
            return PassThroughConfigTests.mutateForVersion(passThroughConfig, version);
        } else if (inferenceConfig instanceof TextEmbeddingConfig textEmbeddingConfig) {
            return TextEmbeddingConfigTests.mutateForVersion(textEmbeddingConfig, version);
        } else if (inferenceConfig instanceof TextSimilarityConfig textSimilarityConfig) {
            return TextSimilarityConfigTests.mutateForVersion(textSimilarityConfig, version);
        } else if (inferenceConfig instanceof ZeroShotClassificationConfig zeroShotClassificationConfig) {
            return ZeroShotClassificationConfigTests.mutateForVersion(zeroShotClassificationConfig, version);
        } else if (inferenceConfig instanceof TextExpansionConfig textExpansionConfig) {
            return TextExpansionConfigTests.mutateForVersion(textExpansionConfig, version);
        } else {
            throw new IllegalArgumentException("unknown inference config [" + inferenceConfig.getName() + "]");
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(new MlInferenceNamedXContentProvider().getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    @Override
    protected List<TransportVersion> bwcVersions() {
        T obj = createTestInstance();
        return getAllBWCVersions().stream().filter(v -> v.onOrAfter(obj.getMinimalSupportedVersion())).collect(Collectors.toList());
    }
}
