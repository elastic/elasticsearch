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
 * This package contains tests that use mustache to test what looks
 * to be unrelated functionality, or functionality that should be 
 * tested with a mock instead. Instead of doing an epic battle
 * with these tests, they are temporarily moved here to the mustache
 * module's tests, but that is likely not where they belong. Please 
 * help by cleaning them up and we can remove this package!
 *
 * <ul>
 *   <li>If the test is actually testing mustache specifically, move to 
 *       the org.elasticsearch.script.mustache tests package of this module</li>
 *   <li>If the test is testing templating integration with another core subsystem,
 *       fix it to use a mock instead, so it can be in the core tests again</li>
 *   <li>If the test is just being lazy, and does not really need templating to test
 *       something, clean it up!</li>
 * </ul>
 */
/* List of renames that took place:  
renamed:    core/src/test/java/org/elasticsearch/validate/RenderSearchTemplateIT.java -> modules/lang-mustache/src/test/java/org/elasticsearch/messy/tests/RenderSearchTemplateTests.java
renamed:    core/src/test/java/org/elasticsearch/search/suggest/SuggestSearchIT.java -> modules/lang-mustache/src/test/java/org/elasticsearch/messy/tests/SuggestSearchTests.java
renamed:    core/src/test/java/org/elasticsearch/index/query/TemplateQueryParserTests.java -> modules/lang-mustache/src/test/java/org/elasticsearch/messy/tests/TemplateQueryParserTests.java
renamed:    core/src/test/java/org/elasticsearch/index/query/TemplateQueryIT.java -> modules/lang-mustache/src/test/java/org/elasticsearch/messy/tests/TemplateQueryTests.java
renamed:    core/src/test/java/org/elasticsearch/transport/ContextAndHeaderTransportIT.java -> module/lang-mustache/src/test/java/org/elasticsearch/messy/tests/ContextAndHeaderTransportTests.java
  ^^^^^ note: just the methods from this test using mustache were moved here, the others use groovy and are in the groovy module under its messy tests package.
renamed:    rest-api-spec/test/msearch/10_basic.yaml -> module/lang-mustache/src/test/resources/rest-api-spec/test/lang_mustache/50_messy_test_msearch.yaml 
 */

package org.elasticsearch.messy.tests;
