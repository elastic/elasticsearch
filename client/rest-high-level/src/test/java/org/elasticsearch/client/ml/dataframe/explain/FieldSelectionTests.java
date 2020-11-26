/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
