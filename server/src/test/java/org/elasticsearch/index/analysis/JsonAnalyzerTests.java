package org.elasticsearch.index.analysis;

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

import com.fasterxml.jackson.core.JsonParseException;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.elasticsearch.index.analysis.JsonAnalyzer;
import org.elasticsearch.test.ESTokenStreamTestCase;

import java.io.IOException;

public class JsonAnalyzerTests extends ESTokenStreamTestCase {

    public void testTextValues() throws Exception {
        assertAnalyzesTo(
            "{ \"key1\": \"value1\", \"key2\": \"value2\" }",
            new String[] { "value1", "value2" });
    }

    public void testNumericValues() throws Exception {
        assertAnalyzesTo(
            "{ \"key\": 2.718 }",
            new String[] { "2.718" });
    }

    public void testBooleanValues() throws Exception {
        assertAnalyzesTo(
            "{ \"key\": false }",
            new String[] { "false" });
    }

    public void testNullValues() throws Exception {
        assertAnalyzesTo(
            "{ \"key\": null }",
            new String[] { "null" });
    }

    public void testArrays() throws Exception {
        assertAnalyzesTo(
            "{ \"key\": [true, false] }",
            new String[] { "true", "false"});
    }

    public void testNestedObjects() throws Exception {
        String input = "{ \"parent1\": { \"key\" : \"value\" }," +
            "\"parent2\": { \"key\" : \"value\" }}";
        assertAnalyzesTo(input, new String[]{"value", "value"});
    }

    public void testMalformedJson() {
        String input = "{ \"key1\": \"value1\", \"key2\"}";
        expectThrows(JsonParseException.class, () -> assertAnalyzesTo(input, null));
    }

    /**
     * A substitute for {@link BaseTokenStreamTestCase#assertAnalyzesTo} that doesn't wrap the
     * input in a mock char filter. The mock filter may emit duplicate characters, which can
     * result in invalid JSON and cause the analyzer to fail. Note that unlike the original
     * assert method, we aren't able to call {@link BaseTokenStreamTestCase#checkResetException}
     * as it's package private.
     *
     * TODO: add a version of BaseTokenStreamTestCase.assertAnalyzesTo that sets useCharFilter to false.
     */
    public static void assertAnalyzesTo(String input, String[] output) throws IOException {
        Analyzer analyzer = new JsonAnalyzer();
        checkAnalysisConsistency(random(), analyzer, false, input);
        assertTokenStreamContents(analyzer.tokenStream("field", input), output);
    }
}
