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

package org.elasticsearch.util.component;

import org.elasticsearch.util.logging.Loggers;
import org.elasticsearch.util.settings.Settings;
import org.slf4j.Logger;

/**
 * @author kimchy (Shay Banon)
 */
public class AbstractComponent {

    protected final Logger logger;

    protected final Settings settings;

    protected final Settings componentSettings;

    public AbstractComponent(Settings settings) {
        this.logger = Loggers.getLogger(getClass(), settings);
        this.settings = settings;
        this.componentSettings = settings.getComponentSettings(getClass());
    }

    public AbstractComponent(Settings settings, Class componentClass) {
        this.logger = Loggers.getLogger(componentClass, settings);
        this.settings = settings;
        this.componentSettings = settings.getComponentSettings(componentClass);
    }

    public String nodeName() {
        return settings.get("name", "");
    }
}
