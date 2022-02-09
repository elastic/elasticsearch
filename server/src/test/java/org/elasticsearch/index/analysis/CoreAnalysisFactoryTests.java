/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        super(new AnalysisPlugin() {
        });
    }
}
