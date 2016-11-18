/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.utils;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xpack.prelert.job.results.Influencer;
import org.elasticsearch.xpack.prelert.support.AbstractWireSerializingTestCase;

public class SingleDocumentTests extends AbstractWireSerializingTestCase<SingleDocument<Influencer>> {

    public void testConstructorWithNullDocument() {
        assertFalse(new SingleDocument<>("string", null).isExists());
    }

    @Override
    protected SingleDocument<Influencer> createTestInstance() {
        SingleDocument<Influencer> document;
        if (randomBoolean()) {
            document = SingleDocument.empty(randomAsciiOfLengthBetween(1, 20));
        } else {
            document = new SingleDocument<Influencer>(randomAsciiOfLengthBetween(1, 20),
                    new Influencer(randomAsciiOfLengthBetween(1, 20), randomAsciiOfLengthBetween(1, 20),
                            randomAsciiOfLengthBetween(1, 20)));
        }
        return document;
    }

    @Override
    protected Reader<SingleDocument<Influencer>> instanceReader() {
        return (in) -> new SingleDocument<>(in, Influencer::new);
    }
}
