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

package org.elasticsearch.index.analysis;

import org.elasticsearch.indices.analysis.AnalysisFactoryTestCase;
import org.elasticsearch.plugins.AnalysisPlugin;

/**
 * Checks on the analysis components that are part of core to make sure that any that are added
 * to lucene are either enabled or explicitly not enabled. During the migration of analysis
 * components to the {@code analysis-common} module this test ignores many components that are
 * available to es-core but mapping in {@code analysis-common}. When the migration is complete
 * no such ignoring will be needed because the analysis components won't be available to core.
 */
public class CoreAnalysisFactoryTests extends AnalysisFactoryTestCase {
    public CoreAnalysisFactoryTests() {
        // Use an empty plugin that doesn't define anything so the test doesn't need a ton of null checks.
        super(new AnalysisPlugin() {});
    }
}
