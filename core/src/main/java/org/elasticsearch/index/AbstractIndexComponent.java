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
        this.logger = Loggers.getLogger(getClass(), indexSettings.getSettings(), indexSettings.getIndex());
        this.deprecationLogger = new DeprecationLogger(logger);
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
