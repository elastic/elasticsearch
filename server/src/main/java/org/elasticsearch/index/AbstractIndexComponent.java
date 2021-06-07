/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;

public abstract class AbstractIndexComponent implements IndexComponent {

    protected final Logger logger;
    protected final DeprecationLogger deprecationLogger;
    protected final IndexSettings indexSettings;

    /**
     * Constructs a new index component, with the index name and its settings.
     */
    protected AbstractIndexComponent(IndexSettings indexSettings) {
        this.logger = Loggers.getLogger(getClass(), indexSettings.getIndex());
        this.deprecationLogger = DeprecationLogger.getLogger(getClass());
        this.indexSettings = indexSettings;
    }

    @Override
    public Index index() {
        return indexSettings.getIndex();
    }

    public IndexSettings getIndexSettings() {
        return indexSettings;
    }
}
