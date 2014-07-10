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
package org.elasticsearch.action.index;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;
import static org.hamcrest.Matchers.equalTo;

/**
  */
public class IndexRequestTests extends ElasticsearchTestCase {

    @Test
    public void testIndexRequestOpTypeFromString() throws Exception {
        String create = "create";
        String index = "index";
        String createUpper = "CREATE";
        String indexUpper = "INDEX";

        assertThat(IndexRequest.OpType.fromString(create), equalTo(IndexRequest.OpType.CREATE));
        assertThat(IndexRequest.OpType.fromString(index), equalTo(IndexRequest.OpType.INDEX));
        assertThat(IndexRequest.OpType.fromString(createUpper), equalTo(IndexRequest.OpType.CREATE));
        assertThat(IndexRequest.OpType.fromString(indexUpper), equalTo(IndexRequest.OpType.INDEX));
    }

    @Test(expected= ElasticsearchIllegalArgumentException.class)
    public void testReadBogusString(){
        String foobar = "foobar";
        IndexRequest.OpType.fromString(foobar);
    }
}
