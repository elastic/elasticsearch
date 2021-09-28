/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.LenientlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.StrictlyParsedTrainedModel;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.EnsembleTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.tree.TreeTests;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.FrequencyEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.LenientlyParsedPreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncodingTests;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.PreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.StrictlyParsedPreProcessor;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.TargetMeanEncodingTests;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

//TODO these tests are temporary until the named objects are actually used by an encompassing class (i.e. ModelInferer)
public class NamedXContentObjectsTests extends AbstractXContentTestCase<NamedXContentObjectsTests.NamedObjectContainer> {

    static class NamedObjectContainer implements ToXContentObject {

        static ParseField PRE_PROCESSORS = new ParseField("pre_processors");
        static ParseField TRAINED_MODEL = new ParseField("trained_model");

        static final ObjectParser<NamedObjectContainer, Void> STRICT_PARSER = createParser(false);
        static final ObjectParser<NamedObjectContainer, Void> LENIENT_PARSER = createParser(true);

        @SuppressWarnings("unchecked")
        private static ObjectParser<NamedObjectContainer, Void> createParser(boolean lenient) {
            ObjectParser<NamedObjectContainer, Void> parser = new ObjectParser<>(
                "named_xcontent_object_container_test",
                lenient,
                NamedObjectContainer::new);
            parser.declareNamedObjects(NamedObjectContainer::setPreProcessors,
                (p, c, n) ->
                lenient ? p.namedObject(LenientlyParsedPreProcessor.class, n, null) :
                    p.namedObject(StrictlyParsedPreProcessor.class, n, null),
                (noc) -> noc.setUseExplicitPreprocessorOrder(true), PRE_PROCESSORS);
            parser.declareNamedObjects(NamedObjectContainer::setTrainedModel,
                (p, c, n) ->
                    lenient ? p.namedObject(LenientlyParsedTrainedModel.class, n, null) :
                        p.namedObject(StrictlyParsedTrainedModel.class, n, null),
                TRAINED_MODEL);
            return parser;
        }

        private boolean useExplicitPreprocessorOrder = false;
        private List<? extends PreProcessor> preProcessors;
        private TrainedModel trainedModel;

        void setPreProcessors(List<? extends PreProcessor> preProcessors) {
            this.preProcessors = preProcessors;
        }

        void setTrainedModel(List<? extends TrainedModel> trainedModel) {
            this.trainedModel = trainedModel.get(0);
        }

        void setModel(TrainedModel trainedModel) {
            this.trainedModel = trainedModel;
        }

        void setUseExplicitPreprocessorOrder(boolean value) {
            this.useExplicitPreprocessorOrder = value;
        }

        static NamedObjectContainer fromXContent(XContentParser parser, boolean lenient) {
            return lenient ? LENIENT_PARSER.apply(parser, null) : STRICT_PARSER.apply(parser, null);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            writeNamedObjects(builder, params, useExplicitPreprocessorOrder, PRE_PROCESSORS.getPreferredName(), preProcessors);
            writeNamedObjects(builder, params, false, TRAINED_MODEL.getPreferredName(), Collections.singletonList(trainedModel));
            builder.endObject();
            return builder;
        }

        XContentBuilder writeNamedObjects(XContentBuilder builder,
                                          Params params,
                                          boolean useExplicitOrder,
                                          String namedObjectsName,
                                          List<? extends NamedXContentObject> namedObjects) throws IOException {
            if (useExplicitOrder) {
                builder.startArray(namedObjectsName);
            } else {
                builder.startObject(namedObjectsName);
            }
            for (NamedXContentObject object : namedObjects) {
                if (useExplicitOrder) {
                    builder.startObject();
                }
                builder.field(object.getName(), object, params);
                if (useExplicitOrder) {
                    builder.endObject();
                }
            }
            if (useExplicitOrder) {
                builder.endArray();
            } else {
                builder.endObject();
            }
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NamedObjectContainer that = (NamedObjectContainer) o;
            return Objects.equals(preProcessors, that.preProcessors) && Objects.equals(trainedModel, that.trainedModel);
        }

        @Override
        public int hashCode() {
            return Objects.hash(preProcessors);
        }
    }

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    public NamedObjectContainer createTestInstance() {
        int max = randomIntBetween(1, 10);
        List<PreProcessor> preProcessors = new ArrayList<>(max);
        for (int i = 0; i < max; i++) {
            preProcessors.add(randomFrom(FrequencyEncodingTests.createRandom(),
                OneHotEncodingTests.createRandom(),
                TargetMeanEncodingTests.createRandom()));
        }
        NamedObjectContainer container = new NamedObjectContainer();
        container.setPreProcessors(preProcessors);
        container.setUseExplicitPreprocessorOrder(true);
        container.setModel(randomFrom(TreeTests.createRandom(), EnsembleTests.createRandom()));
        return container;
    }

    @Override
    protected NamedObjectContainer doParseInstance(XContentParser parser) throws IOException {
        return NamedObjectContainer.fromXContent(parser, lenient);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // We only want to add random fields to the root, or the root of the named objects
        return field ->
                (field.endsWith("frequency_encoding") ||
                    field.endsWith("one_hot_encoding") ||
                    field.endsWith("target_mean_encoding") ||
                    field.startsWith("tree.tree_structure") ||
                    field.isEmpty()) == false;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }
}

