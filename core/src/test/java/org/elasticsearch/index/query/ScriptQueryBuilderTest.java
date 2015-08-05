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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class ScriptQueryBuilderTest extends BaseQueryTestCase<ScriptQueryBuilder> {

    @Override
    protected ScriptQueryBuilder doCreateTestQueryBuilder() {
        String script;
        Map<String, Object> params = null;
        if (randomBoolean()) {
            script = "5 * 2 > param";
            params = new HashMap<>();
            params.put("param", 1);
        } else {
            script = "5 * 2 > 2";
        }
        return new ScriptQueryBuilder(new Script(script, ScriptType.INLINE, "expression", params));
    }

    @Override
    protected void doAssertLuceneQuery(ScriptQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(ScriptQueryBuilder.ScriptQuery.class));
    }

    @Test
    public void testValidate() {
        ScriptQueryBuilder scriptQueryBuilder = new ScriptQueryBuilder(null);
        assertThat(scriptQueryBuilder.validate().validationErrors().size(), is(1));
    }
}
