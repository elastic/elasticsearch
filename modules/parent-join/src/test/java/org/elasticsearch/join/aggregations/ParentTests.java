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

import java.util.Arrays;
import java.util.Collection;

import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.test.TestGeoShapeFieldMapperPlugin;

public class ParentTests extends BaseAggregationTestCase<ParentAggregationBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(ParentJoinPlugin.class, TestGeoShapeFieldMapperPlugin.class);
    }

    @Override
    protected ParentAggregationBuilder createTestAggregatorBuilder() {
        String name = randomAlphaOfLengthBetween(3, 20);
        String parentType = randomAlphaOfLengthBetween(5, 40);
        return new ParentAggregationBuilder(name, parentType);
    }
}
