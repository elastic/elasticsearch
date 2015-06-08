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

package org.elasticsearch.river;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

/**
 * @deprecated See blog post https://www.elastic.co/blog/deprecating_rivers
 */
@Deprecated
public class AbstractRiverComponent implements RiverComponent {

    protected final ESLogger logger;

    protected final RiverName riverName;

    protected final RiverSettings settings;

    protected AbstractRiverComponent(RiverName riverName, RiverSettings settings) {
        this.riverName = riverName;
        this.settings = settings;

        this.logger = Loggers.getLogger(getClass(), settings.globalSettings(), riverName);
    }

    @Override
    public RiverName riverName() {
        return riverName;
    }

    public String nodeName() {
        return settings.globalSettings().get("name", "");
    }
}
