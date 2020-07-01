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
 * <p>
 * This package holds shared code for the aggregations framework, especially around dealing with values.
 * </p>
 *
 * <h2> Key Classes </h2>
 *
 * <h3> {@link org.elasticsearch.search.aggregations.support.ValuesSource} and its subclasses </h3>
 * <p>
 * These are thin wrappers which provide a unified interface to different ways of getting input data (e.g. DocValues from Lucene, or script
 * output). A class hierarchy defines the type of values returned by the source.  The top level sub-classes define type-specific behavior,
 * such as {@link org.elasticsearch.search.aggregations.support.ValuesSource.Numeric#isFloatingPoint()}.  Second level subclasses are
 * then specialized based on where they read values from, e.g. script or field cases.  There are also adapter classes like
 * {@link org.elasticsearch.search.aggregations.bucket.geogrid.CellIdSource} which do run-time conversion from one type to another, often
 * dependent on a user specified parameter (precision in that case).
 * </p>
 *
 * <h3> {@link org.elasticsearch.search.aggregations.support.ValuesSourceRegistry} </h3>
 * <p>
 * ValuesSourceRegistry stores the mappings for what types are supported by what aggregations.  It is configured at startup, when
 * {@link org.elasticsearch.search.SearchModule} is configuring aggregations.  It shouldn't be necessary to access the registry in most
 * cases, but you can get a read copy from {@link org.elasticsearch.index.query.QueryShardContext#getValuesSourceRegistry()} if necessary.
 * </p>
 *
 * <h3> {@link org.elasticsearch.search.aggregations.support.ValuesSourceType} </h3>
 * <p>
 * ValuesSourceTypes are the quantum of support in the aggregations framework, and provide a common language between fields and
 * aggregations.  Fields which support aggregation set a ValuesSourceType on their {@link org.elasticsearch.index.fielddata.IndexFieldData}
 * implementations, and aggregations register what types they support via one of the
 * {@link org.elasticsearch.search.aggregations.support.ValuesSourceRegistry.Builder#register} methods.  The VaulesSourceType itself holds
 * information on how to with values of that type, including methods for creating
 * {@link org.elasticsearch.search.aggregations.support.ValuesSource} instances and {@link org.elasticsearch.search.DocValueFormat}
 * instances.
 * </p>
 *
 * <h3> {@link org.elasticsearch.search.aggregations.support.ValuesSourceConfig} </h3>
 * <p>
 * There are two things going on in ValuesSourceConfig.  First, there is a collection of static factory methods to build valid configs for
 * different situations.  {@link org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder#resolveConfig} has a good
 * default for what to call here and generally aggregations shouldn't need to deviate from that.
 * </p>
 *
 * <p>
 * Once properly constructed, the ValuesSourceConfig provides access to the ValuesSource instance, as well as related information like the
 * formatter.  Aggregations are free to use this information as needed, such as Max and Min which inspect the field context to see if they
 * can apply an optimization.
 * </p>
 *
 * <h2> Classes we are trying to phase out </h2>
 * <h3> {@link org.elasticsearch.search.aggregations.support.ValueType} </h3>
 * <p>
 * This class is primarily used for parsing user type hints, and is deprecated for new use.  Work is ongoing to remove it from existing
 * code.
 * </p>
 *
 */
