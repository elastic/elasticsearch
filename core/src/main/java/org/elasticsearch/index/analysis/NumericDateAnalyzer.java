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

import com.carrotsearch.hppc.IntObjectHashMap;
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class NumericDateAnalyzer extends NumericAnalyzer<NumericDateTokenizer> {

    private static final Map<String, IntObjectHashMap<NamedAnalyzer>> globalAnalyzers = new HashMap<>();

    public static synchronized NamedAnalyzer buildNamedAnalyzer(FormatDateTimeFormatter formatter, int precisionStep) {
        IntObjectHashMap<NamedAnalyzer> precisionMap = globalAnalyzers.get(formatter.format());
        if (precisionMap == null) {
            precisionMap = new IntObjectHashMap<>();
            globalAnalyzers.put(formatter.format(), precisionMap);
        }
        NamedAnalyzer namedAnalyzer = precisionMap.get(precisionStep);
        if (namedAnalyzer == null) {
            String name = "_date/" + ((precisionStep == Integer.MAX_VALUE) ? "max" : precisionStep);
            namedAnalyzer = new NamedAnalyzer(name, AnalyzerScope.GLOBAL, new NumericDateAnalyzer(precisionStep, formatter.parser()));
            precisionMap.put(precisionStep, namedAnalyzer);
        }
        return namedAnalyzer;
    }

    private final int precisionStep;
    private final DateTimeFormatter dateTimeFormatter;

    public NumericDateAnalyzer(int precisionStep, DateTimeFormatter dateTimeFormatter) {
        this.precisionStep = precisionStep;
        this.dateTimeFormatter = dateTimeFormatter;
    }

    @Override
    protected NumericDateTokenizer createNumericTokenizer(char[] buffer) throws IOException {
        return new NumericDateTokenizer(precisionStep, buffer, dateTimeFormatter);
    }
}