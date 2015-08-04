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

import org.apache.lucene.analysis.NumericTokenStream;
import org.apache.lucene.analysis.NumericTokenStream.NumericTermAttribute;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class NumericAnalyzerTests extends ESTestCase {

    @Test
    public void testAttributeEqual() throws IOException {
        final int precisionStep = 8;
        final double value = randomDouble();
        NumericDoubleAnalyzer analyzer = new NumericDoubleAnalyzer(precisionStep);

        final TokenStream ts1 = analyzer.tokenStream("dummy", String.valueOf(value));
        final NumericTokenStream ts2 = new NumericTokenStream(precisionStep);
        ts2.setDoubleValue(value);
        final NumericTermAttribute numTerm1 = ts1.addAttribute(NumericTermAttribute.class);
        final NumericTermAttribute numTerm2 = ts1.addAttribute(NumericTermAttribute.class);
        final PositionIncrementAttribute posInc1 = ts1.addAttribute(PositionIncrementAttribute.class);
        final PositionIncrementAttribute posInc2 = ts1.addAttribute(PositionIncrementAttribute.class);
        ts1.reset();
        ts2.reset();
        while (ts1.incrementToken()) {
            assertThat(ts2.incrementToken(), is(true));
            assertThat(posInc1, equalTo(posInc2));
            // can't use equalTo directly on the numeric attribute cause it doesn't implement equals (LUCENE-5070)
            assertThat(numTerm1.getRawValue(), equalTo(numTerm2.getRawValue()));
            assertThat(numTerm2.getShift(), equalTo(numTerm2.getShift()));
        }
        assertThat(ts2.incrementToken(), is(false));
        ts1.end();
        ts2.end();
    }

}
