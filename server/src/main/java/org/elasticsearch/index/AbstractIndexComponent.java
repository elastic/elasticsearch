/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.logging.Loggers;

public abstract class AbstractIndexComponent {

    protected final Logger logger;
    protected final IndexSettings indexSettings;
    private volatile Index renamedTo = null;

    /**
     * Constructs a new index component, with the index name and its settings.
     */
    protected AbstractIndexComponent(IndexSettings indexSettings) {
        this.logger = Loggers.getLogger(getClass(), indexSettings.getIndex());
        this.indexSettings = indexSettings;
    }

    public Index index() {
        return indexSettings.getIndex();
    }

    public IndexSettings getIndexSettings() {
        return indexSettings;
    }

    /**
     * Updates the renamed name of this IndexService
     *
     * @param newName
     * @param newIndexMetadata
     */
    public void renameTo(String newName, IndexMetadata newIndexMetadata) {
        renamedTo = new Index(newName, index().getUUID());
    }

    /**
     * @return if this index service has been renamed
     */
    public boolean isRenamed() {
        return renamedTo != null;
    }
}
