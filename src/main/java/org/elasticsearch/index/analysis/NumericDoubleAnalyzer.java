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

import com.carrotsearch.hppc.IntObjectOpenHashMap;

import java.io.IOException;

/**
 *
 */
public class NumericDoubleAnalyzer extends NumericAnalyzer<NumericDoubleTokenizer> {

    private final static IntObjectOpenHashMap<NamedAnalyzer> builtIn;

    static {
        builtIn = new IntObjectOpenHashMap<>();
        builtIn.put(Integer.MAX_VALUE, new NamedAnalyzer("_double/max", AnalyzerScope.GLOBAL, new NumericDoubleAnalyzer(Integer.MAX_VALUE)));
        for (int i = 0; i <= 64; i += 4) {
            builtIn.put(i, new NamedAnalyzer("_double/" + i, AnalyzerScope.GLOBAL, new NumericDoubleAnalyzer(i)));
        }
    }

    public static NamedAnalyzer buildNamedAnalyzer(int precisionStep) {
        NamedAnalyzer namedAnalyzer = builtIn.get(precisionStep);
        if (namedAnalyzer == null) {
            namedAnalyzer = new NamedAnalyzer("_double/" + precisionStep, AnalyzerScope.INDEX, new NumericDoubleAnalyzer(precisionStep));
        }
        return namedAnalyzer;
    }

    private final int precisionStep;

    public NumericDoubleAnalyzer(int precisionStep) {
        this.precisionStep = precisionStep;
    }

    @Override
    protected NumericDoubleTokenizer createNumericTokenizer(char[] buffer) throws IOException {
        return new NumericDoubleTokenizer(precisionStep, buffer);
    }
}