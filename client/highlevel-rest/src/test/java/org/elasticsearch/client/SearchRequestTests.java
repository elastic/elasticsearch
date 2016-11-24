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

package org.elasticsearch.client;

import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class SearchRequestTests extends ESTestCase {

    public void testContructor() {
        SearchRequest request = new SearchRequest(new SearchSourceBuilder());
        Map<String, String> params = new HashMap<>();
        params.put("foo", "bar");
        request.indices("aaa", "bbb").types("ccc", "ddd").params(params);
        assertThat(Arrays.asList(request.indices()), contains("aaa", "bbb"));
        assertThat(Arrays.asList(request.types()), contains("ccc", "ddd"));
        assertThat(request.params().size(), equalTo(1));
        assertThat(request.params().get("foo"), equalTo("bar"));
    }

}
