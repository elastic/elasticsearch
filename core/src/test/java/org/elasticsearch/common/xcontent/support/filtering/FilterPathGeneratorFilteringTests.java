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

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class FilterPathGeneratorFilteringTests extends ESTestCase {

    private final JsonFactory JSON_FACTORY = new JsonFactory();

    public void testInclusiveFilters() throws Exception {
        final String SAMPLE = "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}";

        assertResult(SAMPLE, "a", true, "{'a':0}");
        assertResult(SAMPLE, "b", true, "{'b':true}");
        assertResult(SAMPLE, "c", true, "{'c':'c_value'}");
        assertResult(SAMPLE, "d", true, "{'d':[0,1,2]}");
        assertResult(SAMPLE, "e", true, "{'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, "h", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "z", true, "");

        assertResult(SAMPLE, "e.f1", true, "{'e':[{'f1':'f1_value'}]}");
        assertResult(SAMPLE, "e.f2", true, "{'e':[{'f2':'f2_value'}]}");
        assertResult(SAMPLE, "e.f*", true, "{'e':[{'f1':'f1_value','f2':'f2_value'}]}");
        assertResult(SAMPLE, "e.*2", true, "{'e':[{'f2':'f2_value'},{'g2':'g2_value'}]}");

        assertResult(SAMPLE, "h.i", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.i.j", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.i.j.k", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.i.j.k.l", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, "h.*", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "*.i", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, "*.i.j", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.*.j", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.i.*", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, "*.i.j.k", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.*.j.k", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.i.*.k", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.i.j.*", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, "*.i.j.k.l", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.*.j.k.l", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.i.*.k.l", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.i.j.*.l", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h.i.j.k.*", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, "h.*.j.*.l", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "**.l", true, "{'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, "**.*2", true, "{'e':[{'f2':'f2_value'},{'g2':'g2_value'}]}");
    }

    public void testExclusiveFilters() throws Exception {
        final String SAMPLE = "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}";

        assertResult(SAMPLE, "a", false, "{'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "b", false, "{'a':0,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "c", false, "{'a':0,'b':true,'d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "d", false, "{'a':0,'b':true,'c':'c_value','e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "e", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "h", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, "z", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, "e.f1", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "e.f2", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value'},{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "e.f*", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'g1':'g1_value','g2':'g2_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");
        assertResult(SAMPLE, "e.*2", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value'},{'g1':'g1_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

        assertResult(SAMPLE, "h.i", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, "h.i.j", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, "h.i.j.k", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, "h.i.j.k.l", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");

        assertResult(SAMPLE, "h.*", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, "*.i", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");

        assertResult(SAMPLE, "*.i.j", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, "h.*.j", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, "h.i.*", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");

        assertResult(SAMPLE, "*.i.j.k", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, "h.*.j.k", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, "h.i.*.k", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, "h.i.j.*", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");

        assertResult(SAMPLE, "*.i.j.k.l", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, "h.*.j.k.l", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, "h.i.*.k.l", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, "h.i.j.*.l", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, "h.i.j.k.*", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");

        assertResult(SAMPLE, "h.*.j.*.l", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");
        assertResult(SAMPLE, "**.l", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value','f2':'f2_value'},{'g1':'g1_value','g2':'g2_value'}]}");

        assertResult(SAMPLE, "**.*2", false, "{'a':0,'b':true,'c':'c_value','d':[0,1,2],'e':[{'f1':'f1_value'},{'g1':'g1_value'}],'h':{'i':{'j':{'k':{'l':'l_value'}}}}}");

    }

    public void testInclusiveFiltersWithDots() throws Exception {
        assertResult("{'a':0,'b.c':'value','b':{'c':'c_value'}}", "b.c", true, "{'b':{'c':'c_value'}}");
        assertResult("{'a':0,'b.c':'value','b':{'c':'c_value'}}", "b\\.c", true, "{'b.c':'value'}");
    }

    public void testExclusiveFiltersWithDots() throws Exception {
        assertResult("{'a':0,'b.c':'value','b':{'c':'c_value'}}", "b.c", false, "{'a':0,'b.c':'value'}");
        assertResult("{'a':0,'b.c':'value','b':{'c':'c_value'}}", "b\\.c", false, "{'a':0,'b':{'c':'c_value'}}");
    }

    private void assertResult(String input, String filter, boolean inclusive, String expected) throws Exception {
        try (BytesStreamOutput os = new BytesStreamOutput()) {
            try (FilteringGeneratorDelegate generator = new FilteringGeneratorDelegate(JSON_FACTORY.createGenerator(os),
                    new FilterPathBasedFilter(Collections.singleton(filter), inclusive), true, true)) {
                try (JsonParser parser = JSON_FACTORY.createParser(replaceQuotes(input))) {
                    while (parser.nextToken() != null) {
                        generator.copyCurrentStructure(parser);
                    }
                }
            }
            assertThat(os.bytes().utf8ToString(), equalTo(replaceQuotes(expected)));
        }
    }

    private String replaceQuotes(String s) {
        return s.replace('\'', '"');
    }
}
