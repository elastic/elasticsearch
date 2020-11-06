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
package org.elasticsearch.client.ml.inference.trainedmodel.ensemble;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class LogisticRegressionTests extends AbstractXContentTestCase<LogisticRegression> {

    LogisticRegression createTestInstance(int numberOfWeights) {
        return new LogisticRegression(Stream.generate(ESTestCase::randomDouble).limit(numberOfWeights).collect(Collectors.toList()));
    }

    @Override
    protected LogisticRegression doParseInstance(XContentParser parser) throws IOException {
        return LogisticRegression.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected LogisticRegression createTestInstance() {
        return randomBoolean() ? new LogisticRegression(null) : createTestInstance(randomIntBetween(1, 100));
    }

}
