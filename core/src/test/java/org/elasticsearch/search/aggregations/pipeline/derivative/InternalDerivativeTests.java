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

package org.elasticsearch.search.aggregations.pipeline.derivative;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.List;
import java.util.Map;

public class InternalDerivativeTests extends InternalAggregationTestCase<InternalDerivative> {

    @Override
    protected InternalDerivative createTestInstance(String name,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        DocValueFormat formatter = randomFrom(DocValueFormat.BOOLEAN, DocValueFormat.GEOHASH,
                DocValueFormat.IP, DocValueFormat.RAW);
        double value = randomDoubleBetween(0, 100000, true);
        double normalizationFactor = randomDoubleBetween(0, 100000, true);
        return new InternalDerivative(name, value, normalizationFactor, formatter,
                pipelineAggregators, metaData);
    }

    @Override
    public void testReduceRandom() {
        // no test since reduce operation is unsupported
    }

    @Override
    protected void assertReduced(InternalDerivative reduced, List<InternalDerivative> inputs) {
        // no test since reduce operation is unsupported
    }

    @Override
    protected Reader<InternalDerivative> instanceReader() {
        return InternalDerivative::new;
    }

}
