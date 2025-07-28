/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.langident;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.inference.MlInferenceNamedXContentProvider;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LangIdentNeuralNetworkTests extends AbstractXContentSerializingTestCase<LangIdentNeuralNetwork> {

    private boolean lenient;

    @Before
    public void chooseStrictOrLenient() {
        lenient = randomBoolean();
    }

    @Override
    protected LangIdentNeuralNetwork doParseInstance(XContentParser parser) throws IOException {
        return lenient ? LangIdentNeuralNetwork.fromXContentLenient(parser) : LangIdentNeuralNetwork.fromXContentStrict(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return lenient;
    }

    @Override
    protected LangIdentNeuralNetwork createTestInstance() {
        return createRandom();
    }

    @Override
    protected LangIdentNeuralNetwork mutateInstance(LangIdentNeuralNetwork instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static LangIdentNeuralNetwork createRandom() {
        return new LangIdentNeuralNetwork(randomAlphaOfLength(10), LangNetLayerTests.createRandom(), LangNetLayerTests.createRandom());
    }

    @Override
    protected Writeable.Reader<LangIdentNeuralNetwork> instanceReader() {
        return LangIdentNeuralNetwork::new;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }

}
