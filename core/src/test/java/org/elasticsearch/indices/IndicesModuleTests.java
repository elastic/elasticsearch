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

import org.apache.lucene.analysis.hunspell.Dictionary;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.query.TermQueryParser;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

public class IndicesModuleTests extends ModuleTestCase {

    static class FakeQueryParser implements QueryParser {
        @Override
        public String[] names() {
            return new String[] {"fake-query-parser"};
        }
        @Override
        public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
            return null;
        }
    }

    public void testRegisterQueryParser() {
        IndicesModule module = new IndicesModule(Settings.EMPTY);
        module.registerQueryParser(FakeQueryParser.class);
        assertSetMultiBinding(module, QueryParser.class, FakeQueryParser.class);
    }

    public void testRegisterQueryParserDuplicate() {
        IndicesModule module = new IndicesModule(Settings.EMPTY);
        try {
            module.registerQueryParser(TermQueryParser.class);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Can't register the same [query_parser] more than once for [" + TermQueryParser.class.getName() + "]");
        }
    }

    public void testRegisterHunspellDictionary() throws Exception {
        IndicesModule module = new IndicesModule(Settings.EMPTY);
        InputStream aff = getClass().getResourceAsStream("/indices/analyze/conf_dir/hunspell/en_US/en_US.aff");
        InputStream dic = getClass().getResourceAsStream("/indices/analyze/conf_dir/hunspell/en_US/en_US.dic");
        Dictionary dictionary = new Dictionary(aff, dic);
        module.registerHunspellDictionary("foo", dictionary);
        assertMapInstanceBinding(module, String.class, Dictionary.class, Collections.singletonMap("foo", dictionary));
    }

    public void testRegisterHunspellDictionaryDuplicate() {
        IndicesModule module = new IndicesModule(Settings.EMPTY);
        try {
            module.registerQueryParser(TermQueryParser.class);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Can't register the same [query_parser] more than once for [" + TermQueryParser.class.getName() + "]");
        }
    }

}
