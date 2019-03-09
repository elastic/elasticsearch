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
package org.elasticsearch.client.ml.job.results;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.util.ArrayList;
import java.util.List;

public class AnomalyCauseTests extends AbstractXContentTestCase<AnomalyCause> {

    @Override
    protected AnomalyCause createTestInstance() {
        AnomalyCause anomalyCause = new AnomalyCause();
        if (randomBoolean()) {
            int size = randomInt(10);
            List<Double> actual = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                actual.add(randomDouble());
            }
            anomalyCause.setActual(actual);
        }
        if (randomBoolean()) {
            int size = randomInt(10);
            List<Double> typical = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                typical.add(randomDouble());
            }
            anomalyCause.setTypical(typical);
        }
        if (randomBoolean()) {
            anomalyCause.setByFieldName(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            anomalyCause.setByFieldValue(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            anomalyCause.setCorrelatedByFieldValue(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            anomalyCause.setOverFieldName(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            anomalyCause.setOverFieldValue(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            anomalyCause.setPartitionFieldName(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            anomalyCause.setPartitionFieldValue(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            anomalyCause.setFunction(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            anomalyCause.setFunctionDescription(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            anomalyCause.setFieldName(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            anomalyCause.setProbability(randomDouble());
        }
        if (randomBoolean()) {
            int size = randomInt(10);
            List<Influence> influencers = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                int fieldValuesSize = randomInt(10);
                List<String> fieldValues = new ArrayList<>(fieldValuesSize);
                for (int j = 0; j < fieldValuesSize; j++) {
                    fieldValues.add(randomAlphaOfLengthBetween(1, 20));
                }
                influencers.add(new Influence(randomAlphaOfLengthBetween(1, 20), fieldValues));
            }
            anomalyCause.setInfluencers(influencers);
        }
        return anomalyCause;
    }

    @Override
    protected AnomalyCause doParseInstance(XContentParser parser) {
        return AnomalyCause.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
