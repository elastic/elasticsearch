/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

/**
 *
 */
public class ShingleTokenFilterFactory extends AbstractTokenFilterFactory {

    private final int maxShingleSize;

    private final boolean outputUnigrams;

    private final boolean outputUnigramsIfNoShingles;

    private String tokenSeparator;

    private int minShingleSize;

    @Inject
    public ShingleTokenFilterFactory(Index index, @IndexSettings Settings indexSettings, @Assisted String name, @Assisted Settings settings) {
        super(index, indexSettings, name, settings);
        maxShingleSize = settings.getAsInt("max_shingle_size", ShingleFilter.DEFAULT_MAX_SHINGLE_SIZE);
        minShingleSize = settings.getAsInt("min_shingle_size", ShingleFilter.DEFAULT_MIN_SHINGLE_SIZE);
        outputUnigrams = settings.getAsBoolean("output_unigrams", true);
        outputUnigramsIfNoShingles = settings.getAsBoolean("output_unigrams_if_no_shingles", false);
        tokenSeparator = settings.get("token_separator", ShingleFilter.TOKEN_SEPARATOR);
    }

    @Override
    public TokenStream create(TokenStream tokenStream) {
        ShingleFilter filter = new ShingleFilter(tokenStream, minShingleSize, maxShingleSize);
        filter.setOutputUnigrams(outputUnigrams);
        filter.setOutputUnigramsIfNoShingles(outputUnigramsIfNoShingles);
        filter.setTokenSeparator(tokenSeparator);
        return filter;
    }
    
    public int getMaxShingleSize() {
        return maxShingleSize;
    }
    
    public int getMinShingleSize() {
        return minShingleSize;
    }
    
    public boolean getOutputUnigrams() {
        return outputUnigrams;
    }
    
    public boolean getOutputUnigramsIfNoShingles() {
        return outputUnigramsIfNoShingles;
    }
}
