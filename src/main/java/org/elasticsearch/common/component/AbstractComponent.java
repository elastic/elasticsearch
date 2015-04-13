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

package org.elasticsearch.common.component;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

/**
 *
 */
public abstract class AbstractComponent {

    protected final ESLogger logger;

    protected final Settings settings;

    public AbstractComponent(Settings settings) {
        this.logger = Loggers.getLogger(getClass(), settings);
        this.settings = settings;
    }

    public AbstractComponent(Settings settings, Class customClass) {
        this.logger = Loggers.getLogger(customClass, settings);
        this.settings = settings;
    }

    /**
     * Returns the nodes name from the settings or the empty string if not set.
     */
    public final String nodeName() {
        return settings.get("name", "");
    }
}
