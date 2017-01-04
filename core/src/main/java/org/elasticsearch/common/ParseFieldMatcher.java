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

package org.elasticsearch.common;

import org.elasticsearch.common.settings.Settings;

/**
 * Matcher to use in combination with {@link ParseField} while parsing requests.
 *
 * @deprecated This class used to be useful to parse in strict mode and emit errors rather than deprecation warnings. Now that we return
 * warnings as response headers all the time, it is no longer useful and will soon be removed. The removal is in progress and there is
 * already no strict mode in fact. Use {@link ParseField} directly.
 */
@Deprecated
public class ParseFieldMatcher {
    public static final ParseFieldMatcher EMPTY = new ParseFieldMatcher(Settings.EMPTY);
    public static final ParseFieldMatcher STRICT = new ParseFieldMatcher(Settings.EMPTY);

    public ParseFieldMatcher(Settings settings) {
        //we don't do anything with the settings argument, this whole class will be soon removed
    }
}
