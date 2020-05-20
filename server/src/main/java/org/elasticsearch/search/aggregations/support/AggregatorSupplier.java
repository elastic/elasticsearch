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
package org.elasticsearch.search.aggregations.support;

/**
 * {@link AggregatorSupplier} serves as a marker for what the {@link ValuesSourceRegistry} holds to construct aggregator instances.
 * The aggregators for each aggregation should all share a signature, and that signature should be used to create an AggregatorSupplier for
 * that aggregation.  Alternatively, if an existing supplier has a matching signature, please re-use that.
 *
 * In many cases, this can be a simple wrapper over the aggregator constructor.  If that is sufficient, please consider the "typecast
 * lambda" syntax:
 *
 * {@code
 * (GeoCentroidAggregatorSupplier) (name, context, parent, valuesSource, pipelineAggregators, metadata) ->
 *                 new GeoCentroidAggregator(name, context, parent, (ValuesSource.GeoPoint) valuesSource, pipelineAggregators, metadata));
 * }
 *
 * The suppliers are responsible for any casting of {@link ValuesSource} that needs to happen.  They must accept a base {@link ValuesSource}
 * instance.  The suppliers may perform additional logic to configure the aggregator as needed, such as in
 * {@link org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory} deciding the execution mode.
 *
 * There is ongoing work to  normalize aggregator constructor signatures, and thus reduce the number of AggregatorSupplier interfaces.
 */
public interface AggregatorSupplier {
}
