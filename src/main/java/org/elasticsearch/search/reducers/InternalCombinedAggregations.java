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

package org.elasticsearch.search.reducers;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

public class InternalCombinedAggregations implements Aggregations {

    private static final Function<InternalAggregation, Aggregation> SUPERTYPE_CAST = new Function<InternalAggregation, Aggregation>() {
        @Override
        public Aggregation apply(InternalAggregation input) {
            return input;
        }
    };

    private final List<InternalAggregation> reductionsList;
    private final InternalAggregations aggregations;
    private Map<String, InternalAggregation> reductionsAsMap;

    public InternalCombinedAggregations(List<InternalAggregation> reductionsList, InternalAggregations aggregations) {
        this.reductionsList = reductionsList;
        this.aggregations = aggregations;
    }

    public void addReduction(InternalAggregation aggregation) {
        this.reductionsList.add(aggregation);
        this.reductionsAsMap = null;
    }

    @Override
    public Iterator<Aggregation> iterator() {
        return new Iterator<Aggregation>() {

            Iterator<Aggregation> aggItr = aggregations.iterator();
            Iterator<InternalAggregation> reduceItr = reductionsList.iterator();
            boolean finishedAggItr = false;

            @Override
            public boolean hasNext() {
                finishedAggItr = !aggItr.hasNext();
                return aggItr.hasNext() || reduceItr.hasNext();
            }

            @Override
            public Aggregation next() {
                if (finishedAggItr) {
                    return reduceItr.next();
                } else {
                    return aggItr.next();
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Not Supported");
            }
        };
    }

    @Override
    public List<Aggregation> asList() {
        throw new UnsupportedOperationException("not implemented"); // NOCOMMIT implement this
    }

    @Override
    public Map<String, Aggregation> asMap() {
        return getAsMap();
    }

    @Override
    public Map<String, Aggregation> getAsMap() {
        throw new UnsupportedOperationException("not implemented"); // NOCOMMIT implement this
    }

    @SuppressWarnings("unchecked")
    @Override
    public <A extends Aggregation> A get(String name) {
        A aggregation = aggregations.get(name);
        if (aggregation == null) {
            aggregation = (A) getReductionsMap().get(name);
        }
        return aggregation;
    }

    private Map<String, Aggregation> getReductionsMap() {
        if (reductionsAsMap == null) {
            Map<String, InternalAggregation> aggregationsAsMap = newHashMap();
            for (InternalAggregation aggregation : reductionsList) {
                aggregationsAsMap.put(aggregation.getName(), aggregation);
            }
            this.reductionsAsMap = aggregationsAsMap;
        }
        return Maps.transformValues(reductionsAsMap, SUPERTYPE_CAST);
    }

    public InternalAggregations getReductions() {
        return new InternalAggregations(reductionsList);
    }

    public Object getProperty(String path) {
        AggregationPath aggPath = AggregationPath.parse(path);
        return getProperty(aggPath.getPathElementsAsStringList());
    }

    public Object getProperty(List<String> path) {
        String aggName = path.get(0);
        InternalAggregation aggregation = get(aggName);
        if (aggregation == null) {
            throw new ElasticsearchIllegalArgumentException("Cannot find an aggregation named [" + aggName + "]");
        }
        return aggregation.getProperty(path.subList(1, path.size()));
    }

}
