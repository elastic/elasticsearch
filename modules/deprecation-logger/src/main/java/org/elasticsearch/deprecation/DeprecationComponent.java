/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.deprecation;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.Loggers;

import static org.elasticsearch.common.logging.DeprecationLogger.DEPRECATION_ONLY;

public class DeprecationComponent extends AbstractLifecycleComponent {
    private final HeaderWarningAppender appender;

    public DeprecationComponent() {
        this.appender = new HeaderWarningAppender("HeaderWarning", DEPRECATION_ONLY);
    }

    @Override
    protected void doStart() {
        Loggers.addAppender(LogManager.getLogger("org.elasticsearch.deprecation"), this.appender);
    }

    @Override
    protected void doStop() {
        Loggers.removeAppender(LogManager.getLogger("org.elasticsearch.deprecation"), this.appender);
    }

    @Override
    protected void doClose() {
        // Nothing to do at present
    }
}
