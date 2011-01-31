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

import org.apache.lucene.analysis.WordlistLoader;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
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

    /**
     * Fetches a list of words from the specified settings file. The list should either be available at the key
     * specified by settingsPrefix or in a file specified by settingsPrefix + _path.
     *
     * @throws ElasticSearchIllegalArgumentException If the word list cannot be found at either key.
     */
    public static Set<String> getWordList(Settings settings, String settingPrefix) {
        String wordListPath = settings.get(settingPrefix + "_path", null);

        if (wordListPath == null) {
            String[] explicitWordList = settings.getAsArray(settingPrefix, null);
            if(explicitWordList == null) {
                String message = String.format("%s or %s_path must be provided.", settingPrefix, settingPrefix);
                throw new ElasticSearchIllegalArgumentException(message);
            } else {

                return new HashSet<String>(Arrays.asList(explicitWordList));
            }
        }

        File wordListFile = new File(wordListPath);
        if (!wordListFile.exists()) {
            throw new ElasticSearchIllegalArgumentException(settingPrefix + "_path file must exist.");
        }

        try {
            return WordlistLoader.getWordSet(wordListFile);
        } catch (IOException ioe) {
            String message = String.format("IOException while reading %s_path: %s", settingPrefix, ioe.getMessage());
            throw new ElasticSearchIllegalArgumentException(message);
        }
    }
}
