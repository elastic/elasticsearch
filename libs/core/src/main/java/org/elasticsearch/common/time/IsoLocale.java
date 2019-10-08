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
package org.elasticsearch.common.time;

import java.util.Locale;

/**
 * Locale constants to be used across elasticsearch code base.
 * java.util.Locale.ROOT should not be used as it defaults start of the week incorrectly to Sunday.
 */
public final class IsoLocale {
    private IsoLocale() {
        throw new UnsupportedOperationException();
    }

    /**
     * We want to use Locale.ROOT but with a start of the week as defined in ISO8601 to be compatible with the behaviour in joda-time
     * https://github.com/elastic/elasticsearch/issues/42588
     * @see java.time.temporal.WeekFields#of(Locale)
      */
    public static final Locale ROOT = new Locale.Builder()
        .setLocale(Locale.ROOT)
        .setUnicodeLocaleKeyword("fw", "mon").build();
}
