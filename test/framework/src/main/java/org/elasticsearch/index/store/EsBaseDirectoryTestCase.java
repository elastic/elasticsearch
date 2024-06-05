/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store;

import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.tests.store.BaseDirectoryTestCase;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.bootstrap.BootstrapForTesting;
import org.elasticsearch.test.junit.listeners.ReproduceInfoPrinter;

/**
 * Extends Lucene's BaseDirectoryTestCase with ES test behavior.
 */
@Listeners({ ReproduceInfoPrinter.class })
@TimeoutSuite(millis = TimeUnits.HOUR)
@LuceneTestCase.SuppressReproduceLine
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
public abstract class EsBaseDirectoryTestCase extends BaseDirectoryTestCase {
    static {
        try {
            Class.forName("org.elasticsearch.test.ESTestCase");
        } catch (ClassNotFoundException e) {
            throw new AssertionError(e);
        }
        BootstrapForTesting.ensureInitialized();
    }

    protected final Logger logger = LogManager.getLogger(getClass());

}
