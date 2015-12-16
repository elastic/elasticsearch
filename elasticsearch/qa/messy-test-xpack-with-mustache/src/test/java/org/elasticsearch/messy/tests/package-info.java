/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
 *   <li>If the test is testing templating integration with another core subsystem,
 *       fix it to use a mock instead, so it can be in the core tests again</li>
 *   <li>If the test is just being lazy, and does not really need templating to test
 *       something, clean it up!</li>
 * </ul>
 */

// renames that took place:
//  renamed:    x-pack/watcher/src/test/java/org/elasticsearch/watcher/test/integration/BasicWatcherTests.java -> qa/messy-test-xpack-with-mustache/src/test/java/org/elasticsearch/messy/tests/BasicWatcherTests.java
//  renamed:    x-pack/watcher/src/test/java/org/elasticsearch/watcher/actions/email/EmailActionIntegrationTests.java -> qa/messy-test-xpack-with-mustache/src/test/java/org/elasticsearch/messy/tests/EmailActionIntegrationTests.java
//  renamed:    x-pack/watcher/src/test/java/org/elasticsearch/watcher/history/HistoryTemplateSearchInputMappingsTests.java -> qa/messy-test-xpack-with-mustache/src/test/java/org/elasticsearch/messy/tests/HistoryTemplateSearchInputMappingsTests.java
//  renamed:    x-pack/watcher/src/test/java/org/elasticsearch/watcher/input/search/SearchInputTests.java -> qa/messy-test-xpack-with-mustache/src/test/java/org/elasticsearch/messy/tests/SearchInputTests.java
//  renamed:    x-pack/watcher/src/test/java/org/elasticsearch/watcher/transform/search/SearchTransformTests.java -> qa/messy-test-xpack-with-mustache/src/test/java/org/elasticsearch/messy/tests/SearchTransformTests.java
//  renamed:    x-pack/shield/src/test/java/org/elasticsearch/integration/ShieldCachePermissionTests.java -> qa/messy-test-xpack-with-mustache/src/test/java/org/elasticsearch/messy/tests/ShieldCachePermissionTests.java
//  renamed:    x-pack/watcher/src/test/java/org/elasticsearch/watcher/actions/TimeThrottleIntegrationTests.java -> qa/messy-test-xpack-with-mustache/src/test/java/org/elasticsearch/messy/tests/TimeThrottleIntegrationTests.java

package org.elasticsearch.messy.tests;
