/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.modelsize;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.FrequencyEncoding;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncoding;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.TargetMeanEncoding;

import java.util.Arrays;
import java.util.List;

public class MlModelSizeNamedXContentProvider implements NamedXContentProvider {
    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return Arrays.asList(
            new NamedXContentRegistry.Entry(PreprocessorSize.class,
                FrequencyEncoding.NAME,
                FrequencyEncodingSize::fromXContent),
            new NamedXContentRegistry.Entry(PreprocessorSize.class,
                OneHotEncoding.NAME,
                OneHotEncodingSize::fromXContent),
            new NamedXContentRegistry.Entry(PreprocessorSize.class,
                TargetMeanEncoding.NAME,
                TargetMeanEncodingSize::fromXContent),
            new NamedXContentRegistry.Entry(TrainedModelSizeInfo.class,
                EnsembleSizeInfo.NAME,
                EnsembleSizeInfo::fromXContent)
        );
    }
}
