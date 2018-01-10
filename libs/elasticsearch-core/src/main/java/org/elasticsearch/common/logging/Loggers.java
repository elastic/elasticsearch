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

import org.apache.logging.log4j.Logger;

public class Loggers {

    public static final String SPACE = " ";

    public static Logger getLogger(Logger parentLogger, String s) {
        assert parentLogger instanceof PrefixLogger;
        return ESLoggerFactory.getLogger(((PrefixLogger)parentLogger).prefix(), parentLogger.getName() + s);
    }

    public static Logger getLogger(String s) {
        return ESLoggerFactory.getLogger(s);
    }

    public static Logger getLogger(Class<?> clazz) {
        return ESLoggerFactory.getLogger(clazz);
    }

    public static Logger getLogger(Class<?> clazz, String... prefixes) {
        return ESLoggerFactory.getLogger(formatPrefix(prefixes), clazz);
    }

    public static Logger getLogger(String name, String... prefixes) {
        return ESLoggerFactory.getLogger(formatPrefix(prefixes), name);
    }

    private static String formatPrefix(String... prefixes) {
        String prefix = null;
        if (prefixes != null && prefixes.length > 0) {
            StringBuilder sb = new StringBuilder();
            for (String prefixX : prefixes) {
                if (prefixX != null) {
                    if (prefixX.equals(SPACE)) {
                        sb.append(" ");
                    } else {
                        sb.append("[").append(prefixX).append("]");
                    }
                }
            }
            if (sb.length() > 0) {
                sb.append(" ");
                prefix = sb.toString();
            }
        }
        return prefix;
    }
}
