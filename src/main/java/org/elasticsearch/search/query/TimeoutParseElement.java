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

package org.elasticsearch.search.query;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.threadpool.ThreadPool;

/**
 */
public class TimeoutParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_NUMBER) {
            context.timeoutInMillis(parser.longValue());
        } else {
            context.timeoutInMillis(TimeValue.parseTimeValue(parser.text(), null).millis());
        }
        if (context.timeoutInMillis() > 0) {
            // Check that the requested timeout setting is within the bounds of
            // what we can detect without false positives - (false negatives are
            // still possible obviously and relate to the frequency with which
            // we are able to perform timeout checks in all sections of our code
            Settings searchSettings = context.indexShard().searchService().indexSettings();
            Settings componentSettings = searchSettings.getComponentSettings(ThreadPool.class);
            TimeValue timerResolution = componentSettings.getAsTime(ThreadPool.Names.ESTIMATED_TIME_INTERVAL,
                    TimeValue.timeValueMillis(ThreadPool.DEFAULT_ESTIMATED_TIME_INTERVAL));
            if (context.timeoutInMillis() < timerResolution.getMillis() * 2) {
                throw new SearchParseException(context,
                        "Search timeout value must be at least double the node's configured timer resolution (currently "
                                + timerResolution.getMillis() + " milliseconds). This timer resolution is set using the \""
                                + ThreadPool.THREADPOOL_GROUP
                                + ThreadPool.Names.ESTIMATED_TIME_INTERVAL + "\" node setting");
            }

        }
    }
}

