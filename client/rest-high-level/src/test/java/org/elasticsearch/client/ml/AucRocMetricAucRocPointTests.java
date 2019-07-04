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

public class AucRocMetricAucRocPointTests extends AbstractXContentTestCase<AucRocMetric.AucRocPoint> {

    static AucRocMetric.AucRocPoint randomPoint() {
        return new AucRocMetric.AucRocPoint(randomDouble(), randomDouble(), randomDouble());
    }

    @Override
    protected AucRocMetric.AucRocPoint createTestInstance() {
        return randomPoint();
    }

    @Override
    protected AucRocMetric.AucRocPoint doParseInstance(XContentParser parser) throws IOException {
        return AucRocMetric.AucRocPoint.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
