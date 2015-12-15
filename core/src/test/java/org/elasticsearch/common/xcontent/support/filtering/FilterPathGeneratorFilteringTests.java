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

package org.elasticsearch.common.xcontent.support.filtering;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.filter.FilteringGeneratorDelegate;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class FilterPathGeneratorFilteringTests extends ESTestCase {

    private final JsonFactory JSON_FACTORY = new JsonFactory();

    public void testFilters() throws Exception {
        final String SAMPLE = "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}";

        assertResult(SAMPLE, "a", "{'a':0}");
        assertResult(SAMPLE, "b", "{'b':true}");
        assertResult(SAMPLE, "c", "{'c':'c_value'}");
        assertResult(SAMPLE, "d", "{'d':[0,1,2]}");
        assertResult(SAMPLE, "e", "{'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, "h", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "z", "");

        assertResult(SAMPLE, "e.f1", "{'e':[{'f1':'f1_value'}]}");
        assertResult(SAMPLE, "e.f2", "{'e':[{'f2':'f2_value'}]}");
        assertResult(SAMPLE, "e.f*", "{'e':[{'f1':'f1_value','f2':'f2_value'}]}");
        assertResult(SAMPLE, "e.*2", "{'e':[{'f2':'f2_value'},{'g2':'g2_value'}]}");

        assertResult(SAMPLE, "h.i", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.i.j", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.i.j.k", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.i.j.k.l", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, "h.*", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "*.i", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, "*.i.j", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.*.j", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.i.*", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, "*.i.j.k", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.*.j.k", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.i.*.k", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.i.j.*", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, "*.i.j.k.l", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.*.j.k.l", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.i.*.k.l", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.i.j.*.l", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.i.j.k.*", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, "h.*.j.*.l", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "**.l", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, "**.*2", "{'e':[{'f2':'f2_value'},{'g2':'g2_value'}]}");
    }

    public void testFiltersWithDots() throws Exception {
        assertResult("{'a':0,'b.c':'value','b':{'c':'c_value'}}", "b.c", "{'b':{'c':'c_value'}}");
        assertResult("{'a':0,'b.c':'value','b':{'c':'c_value'}}", "b\\.c", "{'b.c':'value'}");
    }

    private void assertResult(String input, String filter, String expected) throws Exception {
        try (BytesStreamOutput os = new BytesStreamOutput()) {
            try (FilteringGeneratorDelegate generator = new FilteringGeneratorDelegate(JSON_FACTORY.createGenerator(os), new FilterPathBasedFilter(new String[]{filter}), true, true)) {
                try (JsonParser parser = JSON_FACTORY.createParser(replaceQuotes(input))) {
                    while (parser.nextToken() != null) {
                        generator.copyCurrentStructure(parser);
                    }
                }
            }
            assertThat(os.bytes().toUtf8(), equalTo(replaceQuotes(expected)));
        }
    }

    private String replaceQuotes(String s) {
        return s.replace('\'', '"');
    }
}
