/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.jmx.ManagedGroupName;
import org.elasticsearch.util.logging.ESLogger;
import org.elasticsearch.util.logging.Loggers;
import org.elasticsearch.util.settings.Settings;

import static org.elasticsearch.index.IndexServiceManagement.*;

/**
 * @author kimchy (shay.banon)
 */
public abstract class AbstractIndexComponent implements IndexComponent {

    protected final ESLogger logger;

    protected final Index index;

    protected final Settings indexSettings;

    protected final Settings componentSettings;

    protected AbstractIndexComponent(Index index, @IndexSettings Settings indexSettings) {
        this.index = index;
        this.indexSettings = indexSettings;
        this.componentSettings = indexSettings.getComponentSettings(getClass());

        this.logger = Loggers.getLogger(getClass(), indexSettings, index);
    }

    protected AbstractIndexComponent(Index index, @IndexSettings Settings indexSettings, String prefixSettings) {
        this.index = index;
        this.indexSettings = indexSettings;
        this.componentSettings = indexSettings.getComponentSettings(prefixSettings, getClass());

        this.logger = Loggers.getLogger(getClass(), indexSettings, index);
    }

    @Override public Index index() {
        return this.index;
    }

    public String nodeName() {
        return indexSettings.get("name", "");
    }

    @ManagedGroupName
    private String managementGroupName() {
        return buildIndexGroupName(index);
    }
}