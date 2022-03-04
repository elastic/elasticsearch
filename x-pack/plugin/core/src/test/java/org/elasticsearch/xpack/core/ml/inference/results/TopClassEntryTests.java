/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.results;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class TopClassEntryTests extends AbstractSerializingTestCase<TopClassEntry> {

    public static TopClassEntry createRandomTopClassEntry() {
        Object classification;
        if (randomBoolean()) {
            classification = randomAlphaOfLength(10);
        } else if (randomBoolean()) {
            classification = randomBoolean();
        } else {
            classification = randomDouble();
        }
        return new TopClassEntry(classification, randomDouble(), randomDouble());
    }

    @Override
    protected TopClassEntry doParseInstance(XContentParser parser) throws IOException {
        return TopClassEntry.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<TopClassEntry> instanceReader() {
        return TopClassEntry::new;
    }

    @Override
    protected TopClassEntry createTestInstance() {
        return createRandomTopClassEntry();
    }
}
