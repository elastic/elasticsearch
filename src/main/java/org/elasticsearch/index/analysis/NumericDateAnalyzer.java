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

import org.apache.lucene.util.NumericUtils;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.io.Reader;

/**
 *
 */
public class NumericDateAnalyzer extends NumericAnalyzer<NumericDateTokenizer> {

    private final int precisionStep;

    private final DateTimeFormatter dateTimeFormatter;

    public NumericDateAnalyzer(DateTimeFormatter dateTimeFormatter) {
        this(NumericUtils.PRECISION_STEP_DEFAULT, dateTimeFormatter);
    }

    public NumericDateAnalyzer(int precisionStep, DateTimeFormatter dateTimeFormatter) {
        this.precisionStep = precisionStep;
        this.dateTimeFormatter = dateTimeFormatter;
    }

    @Override
    protected NumericDateTokenizer createNumericTokenizer(Reader reader, char[] buffer) throws IOException {
        return new NumericDateTokenizer(reader, precisionStep, buffer, dateTimeFormatter);
    }
}