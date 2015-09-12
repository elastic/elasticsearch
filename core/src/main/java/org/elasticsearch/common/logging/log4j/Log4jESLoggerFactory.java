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

package org.elasticsearch.common.logging.log4j;

import org.apache.log4j.Logger;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

/**
 *
 */
public class Log4jESLoggerFactory extends ESLoggerFactory {

    @Override
    protected ESLogger rootLogger() {
        return new Log4jESLogger(null, Logger.getRootLogger());
    }

    @Override
    protected ESLogger newInstance(String prefix, String name) {
        final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(name);
        return new Log4jESLogger(prefix, logger);
    }
}
