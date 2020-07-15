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

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.message.Message;
import org.elasticsearch.common.logging.ESLogMessage;
import org.elasticsearch.common.logging.HeaderWarning;

@Plugin(name = "HeaderWarning", category = "Elastic")
public class HeaderWarningAppender extends AbstractAppender {
    public HeaderWarningAppender(String name, Filter filter) {
        super(name, filter, null);
    }

    @Override
    public void append(LogEvent event) {
        final Message message = event.getMessage();

        if (message instanceof ESLogMessage) {
            final ESLogMessage esLogMessage = (ESLogMessage) message;

            String messagePattern = esLogMessage.getMessagePattern();
            Object[] arguments = esLogMessage.getArguments();

            HeaderWarning.addWarning(messagePattern, arguments);
        }
    }
}
