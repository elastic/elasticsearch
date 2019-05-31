/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.dataframe.evaluation.softclassification.AucRocMetric;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.client.ml.AucRocMetricAucRocPointTests.randomPoint;

public class AucRocMetricResultTests extends AbstractXContentTestCase<AucRocMetric.Result> {

    static AucRocMetric.Result randomResult() {
        return new AucRocMetric.Result(
            randomDouble(),
            Stream
                .generate(() -> randomPoint())
                .limit(randomIntBetween(1, 10))
                .collect(Collectors.toList()));
    }

    @Override
    protected AucRocMetric.Result createTestInstance() {
        return randomResult();
    }

    @Override
    protected AucRocMetric.Result doParseInstance(XContentParser parser) throws IOException {
        return AucRocMetric.Result.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // allow unknown fields in the root of the object only
        return field -> !field.isEmpty();
    }
}
