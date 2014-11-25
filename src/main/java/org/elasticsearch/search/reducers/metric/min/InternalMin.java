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

package org.elasticsearch.search.reducers.metric.min;


import org.elasticsearch.search.aggregations.metrics.min.Min;

import java.util.Map;


public class InternalMin extends org.elasticsearch.search.aggregations.metrics.min.InternalMin implements Min {

    public InternalMin(String name, double maxValue, Map<String, Object> metaData) {
        super(name, maxValue, metaData);
    }

    @Override
    public org.elasticsearch.search.aggregations.metrics.min.InternalMin reduce(ReduceContext reduceContext) {
        throw new UnsupportedOperationException("Not supported");
    }

}
