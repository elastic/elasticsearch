/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.explain;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

public class FieldSelectionTests extends AbstractSerializingTestCase<FieldSelection> {

    public static FieldSelection createRandom() {
        Set<String> mappingTypes = randomSubsetOf(randomIntBetween(1, 3), "int", "float", "double", "text", "keyword", "ip").stream()
            .collect(Collectors.toSet());
        FieldSelection.FeatureType featureType = randomBoolean() ? null : randomFrom(FieldSelection.FeatureType.values());
        String reason = randomBoolean() ? null : randomAlphaOfLength(20);
        return new FieldSelection(randomAlphaOfLength(10), mappingTypes, randomBoolean(), randomBoolean(), featureType, reason);
    }

    @Override
    protected FieldSelection createTestInstance() {
        return createRandom();
    }

    @Override
    protected FieldSelection doParseInstance(XContentParser parser) throws IOException {
        return FieldSelection.PARSER.apply(parser, null);
    }

    @Override
    protected Writeable.Reader<FieldSelection> instanceReader() {
        return FieldSelection::new;
    }
}
