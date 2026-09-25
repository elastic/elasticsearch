/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.TestAnalyzer;
import org.elasticsearch.xpack.esql.VersionMode;

import java.util.List;

/**
 * Runs every test once per {@link VersionMode}. Subclasses only add a constructor passing the mode up and build their
 * analyzers through {@link #analyzer()}, so the whole suite plans at one version per test instance.
 */
public abstract class AnalyzerTestCase extends ESTestCase {

    @ParametersFactory(argumentFormatting = "%1$s")
    public static List<Object[]> params() {
        return VersionMode.params();
    }

    protected final VersionMode versionMode;
    protected final TransportVersion minimumVersion;

    protected AnalyzerTestCase(VersionMode versionMode) {
        this.versionMode = versionMode;
        this.minimumVersion = versionMode.version();
    }

    /** An analyzer pinned to this instance's version. */
    protected TestAnalyzer analyzer() {
        return EsqlTestUtils.analyzer().minimumTransportVersion(minimumVersion);
    }
}
