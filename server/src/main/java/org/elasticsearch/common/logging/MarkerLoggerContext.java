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

package org.elasticsearch.common.logging;

import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;

import java.util.concurrent.atomic.AtomicReference;

public class MarkerLoggerContext extends LoggerContext {

    private final AtomicReference<NodeIdListener> nodeIdListener;

    public MarkerLoggerContext(String name, AtomicReference<NodeIdListener>  nodeIdListener) {
        super(name);
        this.nodeIdListener = nodeIdListener;
    }

    @Override
    public Logger getLogger(final String name) {
        Logger logger = getLogger(name, null);
        MarkerLogger marker = new MarkerLogger(logger,nodeIdListener);
        return marker;
    }
}
