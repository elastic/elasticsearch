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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

public abstract class InternalMultiBucketAggregation extends InternalAggregation implements MultiBucketsAggregation {

    public InternalMultiBucketAggregation() {
    }

    public InternalMultiBucketAggregation(String name) {
        super(name);
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else {
            List<? extends Bucket> buckets = getBuckets();
            Object[] propertyArray = new Object[buckets.size()];
            for (int i = 0; i < buckets.size(); i++) {
                propertyArray[i] = buckets.get(i).getProperty(getName(), path);
            }
            return propertyArray;
        }
    }

    public static abstract class InternalBucket implements Bucket {
        public Object getProperty(String containingAggName, List<String> path) {
            Aggregations aggregations = getAggregations();
            String aggName = path.get(0);
            Aggregation aggregation = aggregations.get(aggName);
            if (aggregation == null) {
                throw new ElasticsearchIllegalArgumentException("Cannot find an aggregation named [" + aggName + "] in [" + containingAggName + "]");
            }
            return aggregation.getProperty(path.subList(1, path.size()));
        }
    }
}
