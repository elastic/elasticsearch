/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.frequentitemsets;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.ml.aggs.frequentitemsets.FrequentItemSetCollector.FrequentItemSet;

public class FrequentItemSetWireSerializationTests extends AbstractWireSerializingTestCase<FrequentItemSet> {

    public static FrequentItemSet randomFrequentItemSet() {
        return new FrequentItemSet(
            randomMap(
                2,
                32,
                () -> new Tuple<>(
                    randomAlphaOfLength(4),
                    randomBoolean() ? randomList(1, 10, () -> randomAlphaOfLength(5)) : randomList(1, 10, () -> randomInt())
                )
            ),
            randomNonNegativeLong(),
            randomDouble()
        );
    }

    @Override
    protected Reader<FrequentItemSet> instanceReader() {
        return FrequentItemSet::new;
    }

    @Override
    protected FrequentItemSet createTestInstance() {
        return randomFrequentItemSet();
    }

    @Override
    protected FrequentItemSet mutateInstance(FrequentItemSet instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

}
