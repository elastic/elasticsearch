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
 * This package contains tests that use groovy to test what looks
 * to be unrelated functionality, or functionality that should be 
 * tested with a mock instead. Instead of doing an epic battle
 * with these tests, they are temporarily moved here to the groovy
 * plugin's tests, but that is likely not where they belong. Please 
 * help by cleaning them up and we can remove this package!
 *
 * <ul>
 *   <li>If the test is actually testing groovy specifically, move to 
 *       the org.elasticsearch.script.groovy tests package of this plugin</li>
 *   <li>If the test is testing scripting integration with another core subsystem,
 *       fix it to use a mock instead, so it can be in the core tests again</li>
 *   <li>If the test is just being lazy, and does not really need scripting to test
 *       something, clean it up!</li>
 * </ul>
 */
/* List of renames that took place:
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/metrics/AvgIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/AvgTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/pipeline/BucketScriptIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/BucketScriptTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/pipeline/BucketSelectorIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/BucketSelectorTests.java
  renamed:    core/src/test/java/org/elasticsearch/document/BulkIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/BulkTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/metrics/CardinalityIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/CardinalityTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/child/ChildQuerySearchIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/ChildQuerySearchTests.java
  renamed:    core/src/test/java/org/elasticsearch/transport/ContextAndHeaderTransportIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/ContextAndHeaderTransportTests.java
  ^^^^^ note: the methods from this test using mustache were moved to the mustache module under its messy tests package.
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/bucket/DateHistogramIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/DateHistogramTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/bucket/DateRangeIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/DateRangeTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/bucket/DoubleTermsIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/DoubleTermsTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/EquivalenceIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/EquivalenceTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/metrics/ExtendedStatsIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/ExtendedStatsTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/functionscore/FunctionScoreIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/FunctionScoreTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/geo/GeoDistanceIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/GeoDistanceTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/metrics/HDRPercentileRanksIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/HDRPercentileRanksTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/metrics/HDRPercentilesIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/HDRPercentilesTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/bucket/HistogramIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/HistogramTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/bucket/IPv4RangeIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/IPv4RangeTests.java
  renamed:    core/src/test/java/org/elasticsearch/script/IndexLookupIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/IndexLookupTests.java
  renamed:    core/src/test/java/org/elasticsearch/script/IndexedScriptIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/IndexedScriptTests.java
  renamed:    core/src/test/java/org/elasticsearch/action/IndicesRequestIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/IndicesRequestTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/innerhits/InnerHitsIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/InnerHitsTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/bucket/LongTermsIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/LongTermsTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/metrics/MaxIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/MaxTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/bucket/MinDocCountIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/MinDocCountTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/metrics/MinIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/MinTests.java
  renamed:    core/src/test/java/org/elasticsearch/percolator/PercolatorIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/PercolatorTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/functionscore/RandomScoreFunctionIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/RandomScoreFunctionTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/bucket/RangeIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/RangeTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/scriptfilter/ScriptQuerySearchIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/ScriptQuerySearchTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/metrics/ScriptedMetricIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/ScriptedMetricTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/fields/SearchFieldsIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/SearchFieldsTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/stats/SearchStatsIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/SearchStatsTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/timeout/SearchTimeoutIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/SearchTimeoutTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/bucket/SignificantTermsSignificanceScoreIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/SignificantTermsSignificanceScoreTests.java
  renamed:    core/src/test/java/org/elasticsearch/nested/SimpleNestedIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/SimpleNestedTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/sort/SimpleSortIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/SimpleSortTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/metrics/StatsIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/StatsTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/bucket/StringTermsIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/StringTermsTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/metrics/SumIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/SumTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/metrics/TDigestPercentileRanksIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/TDigestPercentileRanksTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/metrics/TDigestPercentilesIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/TDigestPercentilesTests.java
  renamed:    core/src/test/java/org/elasticsearch/search/aggregations/bucket/TopHitsIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/TopHitsTests.java
  renamed:    core/src/test/java/org/elasticsearch/index/mapper/TransformOnIndexMapperIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/messy/tests/TransformOnIndexMapperTests.java
  renamed:    core/src/main/java/org/elasticsearch/script/groovy/GroovyScriptCompilationException.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/script/groovy/GroovyRestIT.java
  renamed:    core/src/test/java/org/elasticsearch/script/GroovyScriptIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/script/groovy/GroovyScriptTests.java
  renamed:    core/src/test/java/org/elasticsearch/script/GroovySecurityIT.java -> plugins/lang-groovy/src/test/java/org/elasticsearch/script/groovy/GroovySecurityTests.java
  renamed:    core/src/test/resources/org/elasticsearch/search/aggregations/metrics/scripted/conf/scripts/combine_script.groovy -> plugins/lang-groovy/src/test/resources/org/elasticsearch/messy/tests/conf/scripts/combine_script.groovy
  renamed:    core/src/test/resources/org/elasticsearch/search/aggregations/metrics/scripted/conf/scripts/init_script.groovy -> plugins/lang-groovy/src/test/resources/org/elasticsearch/messy/tests/conf/scripts/init_script.groovy
  renamed:    core/src/test/resources/org/elasticsearch/search/aggregations/metrics/scripted/conf/scripts/map_script.groovy -> plugins/lang-groovy/src/test/resources/org/elasticsearch/messy/tests/conf/scripts/map_script.groovy
  renamed:    core/src/test/resources/org/elasticsearch/search/aggregations/metrics/scripted/conf/scripts/reduce_script.groovy -> plugins/lang-groovy/src/test/resources/org/elasticsearch/messy/tests/conf/scripts/reduce_script.groovy
  renamed:    core/src/test/resources/org/elasticsearch/search/aggregations/bucket/config/scripts/significance_script_no_params.groovy -> plugins/lang-groovy/src/test/resources/org/elasticsearch/messy/tests/conf/scripts/significance_script_no_params.groovy
  renamed:    core/src/test/resources/org/elasticsearch/search/aggregations/bucket/config/scripts/significance_script_with_params.groovy -> plugins/lang-groovy/src/test/resources/org/elasticsearch/messy/tests/conf/scripts/significance_script_with_params.groovy
 */
package org.elasticsearch.messy.tests;
