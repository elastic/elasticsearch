/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.preprocessing;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;


public class EntropyTests extends PreProcessingTests<Entropy> {

    @Override
    protected Entropy doParseInstance(XContentParser parser) throws IOException {
        return lenient ?
            Entropy.fromXContentLenient(parser, PreProcessor.PreProcessorParseContext.DEFAULT) :
            Entropy.fromXContentStrict(parser, PreProcessor.PreProcessorParseContext.DEFAULT);
    }

    @Override
    protected Entropy createTestInstance() {
        return createRandom();
    }

    public static Entropy createRandom() {
        return createRandom(randomBoolean());
    }

    public static Entropy createRandom(boolean isCustom) {
        return new Entropy(randomAlphaOfLength(10), randomAlphaOfLength(10), isCustom);
    }

    @Override
    protected Writeable.Reader<Entropy> instanceReader() {
        return Entropy::new;
    }

    public void testEntropyCalculation() {

        String input = "foo";
        String output = "bar";
        Entropy encoding = new Entropy(input, output, randomBoolean());

        assertThat(encoding.entropyCalc(""),closeTo(0.0, 0.00001));
        assertThat(encoding.entropyCalc("aaaaa"),closeTo(0.0, 0.00001));
        assertThat(encoding.entropyCalc("babababababa"),closeTo(1.0, 0.00001));
        assertThat(encoding.entropyCalc("thisisarandomstring"),closeTo(3.32636040, 0.00001));
    }

    public void testInputOutputFields() {
        String field = randomAlphaOfLength(10);
        String output = randomAlphaOfLength(10);
        Entropy encoding = new Entropy(field, output, randomBoolean());
        assertThat(encoding.inputFields(), containsInAnyOrder(field));
        assertThat(encoding.outputFields(), containsInAnyOrder(output));
    }

}
