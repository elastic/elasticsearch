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

    public void testInclusiveFilters() throws Exception {
        final String SAMPLE = "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}";

        assertResult(SAMPLE, true, "a", "{'a':0}");
        assertResult(SAMPLE, true, "b", "{'b':true}");
        assertResult(SAMPLE, true, "c", "{'c':'c_value'}");
        assertResult(SAMPLE, true, "d", "{'d':[0,1,2]}");
        assertResult(SAMPLE, true, "e", "{'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, true, "h", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, true, "z", "");

        assertResult(SAMPLE, true, "e.f1", "{'e':[{'f1':'f1_value'}]}");
        assertResult(SAMPLE, true, "e.f2", "{'e':[{'f2':'f2_value'}]}");
        assertResult(SAMPLE, true, "e.f*", "{'e':[{'f1':'f1_value','f2':'f2_value'}]}");
        assertResult(SAMPLE, true, "e.*2", "{'e':[{'f2':'f2_value'},{'g2':'g2_value'}]}");

        assertResult(SAMPLE, true, "h.i", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, true, "h.i.j", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, true, "h.i.j.k", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, true, "h.i.j.k.l", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, true, "h.*", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, true, "*.i", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, true, "*.i.j", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, true, "h.*.j", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, true, "h.i.*", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, true, "*.i.j.k", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, true, "h.*.j.k", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, true, "h.i.*.k", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, true, "h.i.j.*", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, true, "*.i.j.k.l", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, true, "h.*.j.k.l", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, true, "h.i.*.k.l", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, true, "h.i.j.*.l", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, true, "h.i.j.k.*", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, true, "h.*.j.*.l", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, true, "**.l", "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, true, "**.*2", "{'e':[{'f2':'f2_value'},{'g2':'g2_value'}]}");
    }

    public void testExclusiveFilters() throws Exception {
        final String SAMPLE = "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}";

        assertResult(SAMPLE, false, "a", "{'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, false, "b", "{'a':0,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, false, "c", "{'a':0,'b':true,'d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, false, "d", "{'a':0,'b':true,'c':'c_value','e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, false, "e", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, false, "h", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, false, "z", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, false, "e.f1", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, false, "e.f2", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value'},{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, false, "e.f*", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, false, "e.*2", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value'},{'g1':'g1_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, false, "h.i", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, false, "h.i.j", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, false, "h.i.j.k", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, false, "h.i.j.k.l", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");

        assertResult(SAMPLE, false, "h.*", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, false, "*.i", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");

        assertResult(SAMPLE, false, "*.i.j", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, false, "h.*.j", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, false, "h.i.*", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");

        assertResult(SAMPLE, false, "*.i.j.k", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, false, "h.*.j.k", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, false, "h.i.*.k", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, false, "h.i.j.*", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");

        assertResult(SAMPLE, false, "*.i.j.k.l", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, false, "h.*.j.k.l", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, false, "h.i.*.k.l", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, false, "h.i.j.*.l", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, false, "h.i.j.k.*", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");

        assertResult(SAMPLE, false, "h.*.j.*.l", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, false, "**.l", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");

        assertResult(SAMPLE, false, "**.*2", "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value'},{'g1':'g1_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

    }

    public void testInclusiveFiltersWithDots() throws Exception {
        assertResult("{'a':0,'b.c':'value','b':{'c':'c_value'}}", true, "b.c", "{'b':{'c':'c_value'}}");
        assertResult("{'a':0,'b.c':'value','b':{'c':'c_value'}}", true, "b\\.c", "{'b.c':'value'}");
    }

    public void testExclusiveFiltersWithDots() throws Exception {
        assertResult("{'a':0,'b.c':'value','b':{'c':'c_value'}}", false, "b.c", "{'a':0,'b.c':'value'}");
        assertResult("{'a':0,'b.c':'value','b':{'c':'c_value'}}", false, "b\\.c", "{'a':0,'b':{'c':'c_value'}}");
    }

    private void assertResult(String input, boolean inclusive, String filter, String expected) throws Exception {
        try (BytesStreamOutput os = new BytesStreamOutput()) {
            try (FilteringGeneratorDelegate generator = new FilteringGeneratorDelegate(JSON_FACTORY.createGenerator(os),
                    new FilterPathBasedFilter(inclusive, new String[] { filter }), true, true)) {
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
