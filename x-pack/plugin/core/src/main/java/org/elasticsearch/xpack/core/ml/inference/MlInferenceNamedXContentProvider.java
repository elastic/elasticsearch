/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.plugins.spi.NamedXContentProvider;
import org.elasticsearch.xpack.core.ml.inference.model.LenientlyParsedModel;
import org.elasticsearch.xpack.core.ml.inference.model.Model;
import org.elasticsearch.xpack.core.ml.inference.model.StrictlyParsedModel;
import org.elasticsearch.xpack.core.ml.inference.model.tree.Tree;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.FrequencyEncoding;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.LenientlyParsedPreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncoding;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.PreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.StrictlyParsedPreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.TargetMeanEncoding;

import java.util.ArrayList;
import java.util.List;

public class MlInferenceNamedXContentProvider implements NamedXContentProvider {

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();

        // PreProcessing Lenient
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedPreProcessor.class, OneHotEncoding.NAME,
            OneHotEncoding::fromXContentLenient));
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedPreProcessor.class, TargetMeanEncoding.NAME,
            TargetMeanEncoding::fromXContentLenient));
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedPreProcessor.class, FrequencyEncoding.NAME,
            FrequencyEncoding::fromXContentLenient));

        // PreProcessing Strict
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedPreProcessor.class, OneHotEncoding.NAME,
            OneHotEncoding::fromXContentStrict));
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedPreProcessor.class, TargetMeanEncoding.NAME,
            TargetMeanEncoding::fromXContentStrict));
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedPreProcessor.class, FrequencyEncoding.NAME,
            FrequencyEncoding::fromXContentStrict));

        // Model Lenient
        namedXContent.add(new NamedXContentRegistry.Entry(LenientlyParsedModel.class, Tree.NAME, Tree::fromXContentLenient));

        // Model Strict
        namedXContent.add(new NamedXContentRegistry.Entry(StrictlyParsedModel.class, Tree.NAME, Tree::fromXContentStrict));

        return namedXContent;
    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();

        // PreProcessing
        namedWriteables.add(new NamedWriteableRegistry.Entry(PreProcessor.class, OneHotEncoding.NAME.getPreferredName(),
            OneHotEncoding::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(PreProcessor.class, TargetMeanEncoding.NAME.getPreferredName(),
            TargetMeanEncoding::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(PreProcessor.class, FrequencyEncoding.NAME.getPreferredName(),
            FrequencyEncoding::new));

        // Model
        namedWriteables.add(new NamedWriteableRegistry.Entry(Model.class, Tree.NAME.getPreferredName(), Tree::new));

        return namedWriteables;
    }
}
