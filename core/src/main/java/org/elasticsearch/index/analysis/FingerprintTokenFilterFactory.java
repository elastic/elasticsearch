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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.FingerprintFilter;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;


/**
 *
 */
public class FingerprintTokenFilterFactory extends AbstractTokenFilterFactory {

    private final char separator;
    private final int maxOutputSize;

    public static ParseField SEPARATOR = new ParseField("separator");
    public static ParseField MAX_OUTPUT_SIZE = new ParseField("max_output_size");

    public static final char DEFAULT_SEPARATOR  = ' ';
    public static final int DEFAULT_MAX_OUTPUT_SIZE = 255;

    public FingerprintTokenFilterFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        this.separator = parseSeparator(settings);
        this.maxOutputSize = settings.getAsInt(MAX_OUTPUT_SIZE.getPreferredName(),
            FingerprintTokenFilterFactory.DEFAULT_MAX_OUTPUT_SIZE);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        TokenStream result = tokenStream;
        result = new FingerprintFilter(result, maxOutputSize, separator);
        return result;
    }

    public static char parseSeparator(Settings settings) throws IllegalArgumentException {
        String customSeparator = settings.get(SEPARATOR.getPreferredName());
        if (customSeparator == null) {
            return FingerprintTokenFilterFactory.DEFAULT_SEPARATOR;
        } else if (customSeparator.length() == 1) {
            return customSeparator.charAt(0);
        }

        throw new IllegalArgumentException("Setting [separator] must be a single, non-null character. ["
            + customSeparator + "] was provided.");
    }
}
