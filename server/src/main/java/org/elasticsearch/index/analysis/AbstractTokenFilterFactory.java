/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import org.apache.lucene.util.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;

public abstract class AbstractTokenFilterFactory extends AbstractIndexComponent implements TokenFilterFactory {

    private final String name;

    protected final Version version;

    public AbstractTokenFilterFactory(IndexSettings indexSettings, String name, Settings settings) {
        super(indexSettings);
        this.name = name;
        this.version = Analysis.parseAnalysisVersion(indexSettings, settings, logger);
    }

    @Override
    public String name() {
        return this.name;
    }

    public final Version version() {
        return version;
    }
}
