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

/**
 * The Aggregations Support package holds shared code for the aggregations  framework, especially around dealing with values.
 *
 * Key Classes
 *
 * {@link org.elasticsearch.search.aggregations.support.ValuesSource} and its subclasses
 * These are thin wrappers which provide a unified interface to different ways of getting input data (e.g. DocValues from Lucene, or script
 * output). A class hierarchy defines the type of values returned by the source.  The top level sub-classes define type-specific behavior,
 * such as {@link org.elasticsearch.search.aggregations.support.ValuesSource.Numeric#isFloatingPoint()}.  Second level subclasses are
 * then specialized based on where they read values from, e.g. script or field cases.  There are also adapter classes like
 * {@link org.elasticsearch.search.aggregations.bucket.geogrid.CellIdSource} which do run-time conversion from one type to another, often
 * dependent on a user specified parameter (precision in that case).
 *
 * {@link org.elasticsearch.search.aggregations.support.ValuesSourceRegistry}
 * ValuesSourceRegistry stores the mappings for what types are supported by what aggregations.  It is configured once during startup, when
 * {@link org.elasticsearch.search.SearchModule} is configuring aggregations.  It shouldn't be necessary to access the registry in most
 * cases, but you can get a read copy from {@link org.elasticsearch.index.query.QueryShardContext#getValuesSourceRegistry()} if necessary.
 *
 * {@link org.elasticsearch.search.aggregations.support.ValuesSourceType}
 * ValuesSourceTypes are the quantum of support in the aggregations framework, and provide a common language between fields and
 * aggregations.  Fields which support aggregation override {@link org.elasticsearch.index.mapper.MappedFieldType#getValuesSourceType()} to
 * return a compatible VaulesSourceType (based on how the field is stored), and aggregations register what types they support via one of the
 * {@link org.elasticsearch.search.aggregations.support.ValuesSourceRegistry#register} methods.  The VaulesSourceType itself holds
 * information on how to with values of that type, including methods for creating
 * {@link org.elasticsearch.search.aggregations.support.ValuesSource} instances and {@link org.elasticsearch.search.DocValueFormat}
 * instances.
 *
 * {@link org.elasticsearch.search.aggregations.support.ValuesSourceConfig}
 * There are two things going on in ValuesSourceConfig.  First, there is a collection of static factory methods to build valid configs for
 * different situations.  {@link org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder#resolveConfig} has a good
 * default for what to call here and generally aggregations shouldn't need to deviate from that.
 *
 * Once properly constructed, the ValuesSourceConfig provides access to the ValuesSource instance, as well as related information like the
 * formatter.  Aggregations are free to use this information as needed, such as Max and Min which inspect the field context to see if they
 * can apply an optimization.
 *
 * Classes we are trying to phase out
 * {@link org.elasticsearch.search.aggregations.support.ValueType}
 * This class is primarially used for parsing user type hints.  We hope to deprecate it before 8.0.
 *
 */
package org.elasticsearch.search.aggregations.support;
