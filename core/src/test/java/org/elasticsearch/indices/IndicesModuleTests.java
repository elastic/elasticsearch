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

package org.elasticsearch.indices;

import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.TermQueryParser;

import java.io.IOException;

public class IndicesModuleTests extends ModuleTestCase {

    static class FakeQueryParser implements QueryParser {
        @Override
        public String[] names() {
            return new String[] {"fake-query-parser"};
        }

        @Override
        public QueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
            return null;
        }

        @Override
        public QueryBuilder getBuilderPrototype() {
            return null;
        }
    }

    public void testRegisterQueryParser() {
        IndicesModule module = new IndicesModule();
        module.registerQueryParser(FakeQueryParser.class);
        assertSetMultiBinding(module, QueryParser.class, FakeQueryParser.class);
    }

    public void testRegisterQueryParserDuplicate() {
        IndicesModule module = new IndicesModule();
        try {
            module.registerQueryParser(TermQueryParser.class);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Can't register the same [query_parser] more than once for [" + TermQueryParser.class.getName() + "]");
        }
    }

    public void testRegisterHunspellDictionaryDuplicate() {
        IndicesModule module = new IndicesModule();
        try {
            module.registerQueryParser(TermQueryParser.class);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Can't register the same [query_parser] more than once for [" + TermQueryParser.class.getName() + "]");
        }
    }

}
