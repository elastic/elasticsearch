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

import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.selector.ClassLoaderContextSelector;

import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

public class MarkerLoggerContextSelector extends ClassLoaderContextSelector {

    private final AtomicReference<NodeIdListener>  nodeIdListener ;

    public MarkerLoggerContextSelector(AtomicReference<NodeIdListener> nodeIdListener) {
        this.nodeIdListener = nodeIdListener;
    }

    @Override
    protected LoggerContext createContext(String name, URI configLocation) {
        return new MarkerLoggerContext("Default", nodeIdListener);
    }
}
