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
package org.elasticsearch.client.ml.inference.preprocessing;

import java.io.IOException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.elasticsearch.client.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;


public class MultiTests extends AbstractXContentTestCase<Multi> {

    @Override
    protected Multi doParseInstance(XContentParser parser) throws IOException {
        return Multi.fromXContent(parser);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> !field.isEmpty();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Multi createTestInstance() {
        return createRandom();
    }

    public static Multi createRandom() {
        return new Multi(
            Stream.generate(() -> randomFrom(
                    FrequencyEncodingTests.createRandom(),
                    TargetMeanEncodingTests.createRandom(),
                    OneHotEncodingTests.createRandom(),
                    NGramTests.createRandom())
            ).limit(randomIntBetween(2, 10)).collect(Collectors.toList()),
            randomBoolean() ? null : randomBoolean());
    }

}
