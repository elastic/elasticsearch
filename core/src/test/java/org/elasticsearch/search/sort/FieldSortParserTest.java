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

package org.elasticsearch.search.sort;

import org.apache.lucene.search.Sort;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;
import org.junit.Test;

public class FieldSortParserTest extends ESTestCase {

    private SearchContext createSearchContext() {
        SearchContext context = new TestSearchContext();
        return context;
    }
    
    public Sort parseSortElement(XContentParser parser) throws Exception {
        SearchContext context = createSearchContext();
        SortParseElement element = new SortParseElement();
        element.parse(parser, context);
        return context.sort();
    }
    
    @Test
    public void test() throws Exception {
        String element = 
                "{\n" + 
                "    \"sort\" : [\n" + 
                "        { \"post_date\" : {\"order\" : \"asc\"}},\n" + 
                "        \"user\",\n" + 
                "        { \"name\" : \"desc\" },\n" + 
                "        { \"age\" : \"desc\" },\n" + 
                "        \"_score\"\n" + 
                "    ]" + 
                "}";
        XContentParser parser = XContentFactory.xContent(element).createParser(element);
        parser.nextToken();
        parser.nextToken();
        parser.nextToken();
        parseSortElement(parser);
    }

}
