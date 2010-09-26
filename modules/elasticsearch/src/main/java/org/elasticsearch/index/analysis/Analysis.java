/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.analysis;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;

import java.util.Set;

/**
 * @author kimchy (shay.banon)
 */
public class Analysis {

    public static boolean isNoStopwords(Settings settings) {
        String value = settings.get("stopwords");
        return value != null && "_none_".equals(value);
    }

    public static Set<?> parseStopWords(Settings settings, Set<?> defaultStopWords) {
        String value = settings.get("stopwords");
        if (value != null) {
            if ("_none_".equals(value)) {
                return ImmutableSet.of();
            } else {
                return ImmutableSet.copyOf(Strings.commaDelimitedListToSet(value));
            }
        }
        String[] stopWords = settings.getAsArray("stopwords", null);
        if (stopWords != null) {
            return ImmutableSet.copyOf(Iterators.forArray(stopWords));
        } else {
            return defaultStopWords;
        }
    }
}
