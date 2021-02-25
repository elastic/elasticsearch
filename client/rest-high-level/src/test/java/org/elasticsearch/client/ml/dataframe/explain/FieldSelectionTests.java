/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml.dataframe.explain;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

public class FieldSelectionTests extends AbstractXContentTestCase<FieldSelection> {

    public static FieldSelection createRandom() {
        Set<String> mappingTypes = randomSubsetOf(randomIntBetween(1, 3), "int", "float", "double", "text", "keyword", "ip")
            .stream().collect(Collectors.toSet());
        FieldSelection.FeatureType featureType = randomBoolean() ? null : randomFrom(FieldSelection.FeatureType.values());
        String reason = randomBoolean() ? null : randomAlphaOfLength(20);
        return new FieldSelection(randomAlphaOfLength(10),
            mappingTypes,
            randomBoolean(),
            randomBoolean(),
            featureType,
            reason);
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
    protected boolean supportsUnknownFields() {
        return true;
    }
}
