/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.junit.listeners.ReproduceInfoPrinter;

@Listeners({ ReproduceInfoPrinter.class })
@TimeoutSuite(millis = TimeUnits.HOUR)
@LuceneTestCase.SuppressReproduceLine
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
/**
 * Basic test case for token streams. the assertion methods in this class will
 * run basic checks to enforce correct behavior of the token streams.
 */
public abstract class ESTokenStreamTestCase extends ESTestCase {
    public Settings.Builder newAnalysisSettingsBuilder() {
        return Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current());
    }
}
