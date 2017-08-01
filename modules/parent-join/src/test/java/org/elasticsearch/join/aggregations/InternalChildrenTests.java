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

package org.elasticsearch.join.aggregations;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry.Entry;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalSingleBucketAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.ParsedSingleBucketAggregation;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.EqualsHashCodeTestUtils.MutateFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public class InternalChildrenTests extends InternalSingleBucketAggregationTestCase<InternalChildren> {

    @Override
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        List<Entry> extendedNamedXContents = new ArrayList<>(super.getNamedXContents());
        extendedNamedXContents.add(new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(ChildrenAggregationBuilder.NAME),
                (p, c) -> ParsedChildren.fromXContent(p, (String) c)));
        return extendedNamedXContents ;
    }

    @Override
    protected InternalChildren createTestInstance(String name, long docCount, InternalAggregations aggregations,
            List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalChildren(name, docCount, aggregations, pipelineAggregators, metaData);
    }

    @Override
    protected void extraAssertReduced(InternalChildren reduced, List<InternalChildren> inputs) {
        // Nothing extra to assert
    }

    @Override
    protected Reader<InternalChildren> instanceReader() {
        return InternalChildren::new;
    }

    @Override
    protected Class<? extends ParsedSingleBucketAggregation> implementationClass() {
        return ParsedChildren.class;
    }

    @Override
    protected MutateFunction<InternalChildren> getMutateFunction() {
        return instance -> {
            String name = instance.getName();
            long docCount = instance.getDocCount();
            InternalAggregations aggregations = instance.getAggregations();
            List<PipelineAggregator> pipelineAggregators = instance.pipelineAggregators();
            Map<String, Object> metaData = instance.getMetaData();
            switch (between(0, 3)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                docCount += between(1, 2000);
                break;
            case 2:
                List<InternalAggregation> aggs = new ArrayList<>();
                aggs.add(new InternalMax("new_max", randomDouble(), randomNumericDocValueFormat(), emptyList(), emptyMap()));
                aggs.add(new InternalMin("new_min", randomDouble(), randomNumericDocValueFormat(), emptyList(), emptyMap()));
                aggregations = new InternalAggregations(aggs);
                break;
            case 3:
            default:
                if (metaData == null) {
                    metaData = new HashMap<>(1);
                } else {
                    metaData = new HashMap<>(instance.getMetaData());
                }
                metaData.put(randomAlphaOfLength(15), randomInt());
                break;
            }
            return new InternalChildren(name, docCount, aggregations, pipelineAggregators, metaData);
        };
    }
}
