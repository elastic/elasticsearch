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

package org.elasticsearch.test;

/**
 * A test case with external dependencies that are started by gradle. See {@link org.elasticsearch.test.rest.ESRestTestCase} or
 * ESSokeClientTestCase for tests that depend on a running Elasticsearch cluster or ExampleExternalIT for one that tests a test fixture. The
 * only thing these have in common is that that are run by gradle during the integTest task and that their dependencies are started by
 * gradle before that task is run and shut down after that task is finished.
 */
public class ESExternalDepsTestCase extends ESTestCase {

}
