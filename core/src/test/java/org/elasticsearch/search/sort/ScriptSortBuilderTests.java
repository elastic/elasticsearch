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

import com.google.common.collect.ImmutableMap;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

public class ScriptSortBuilderTests extends ESTestCase {

    private ScriptSortBuilder prepareSSB() {
        ScriptSortBuilder result = new ScriptSortBuilder(new Script("testscript"), "testtype");
        result.setNestedFilter(new TermQueryBuilder("test_term_query", "test_term"));
        return result;
    }

    public void testToXContentWithParams() throws Exception {
        Map<String, String> builderParams = ImmutableMap.of("first_key", "first_value", "second_key", "second_value");
        
        ScriptSortBuilder woBuildParams = prepareSSB();
        XContentBuilder xcWithOut = XContentFactory.contentBuilder(XContentType.JSON);
        woBuildParams.toXContent(xcWithOut, new ToXContent.MapParams(builderParams));

        ScriptSortBuilder withBuildParams = prepareSSB();
        Map<String, Object> params = ImmutableMap.of("first_script_key", new Object(), "second_script_key", "second_script_value");
        withBuildParams.setParams(params);
        XContentBuilder xcWith = XContentFactory.contentBuilder(XContentType.JSON);
        withBuildParams.toXContent(xcWith, new ToXContent.MapParams(builderParams));
        
        assertEquals(withBuildParams.toString(), woBuildParams.toString());
    }
}
