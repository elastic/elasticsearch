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
package org.elasticsearch.search;

import org.elasticsearch.common.inject.ModuleTestCase;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.TermQueryParser;
import org.elasticsearch.search.highlight.CustomHighlighter;
import org.elasticsearch.search.highlight.Highlighter;
import org.elasticsearch.search.highlight.PlainHighlighter;
import org.elasticsearch.search.suggest.CustomSuggester;
import org.elasticsearch.search.suggest.phrase.PhraseSuggester;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
/**
 */
public class SearchModuleTests extends ModuleTestCase {

   public void testDoubleRegister() {
       SearchModule module = new SearchModule(Settings.EMPTY, new NamedWriteableRegistry());
       try {
           module.registerHighlighter("fvh", PlainHighlighter.class);
       } catch (IllegalArgumentException e) {
           assertEquals(e.getMessage(), "Can't register the same [highlighter] more than once for [fvh]");
       }

       try {
           module.registerSuggester("term", PhraseSuggester.INSTANCE);
       } catch (IllegalArgumentException e) {
           assertEquals(e.getMessage(), "Can't register the same [suggester] more than once for [term]");
       }
   }

    public void testRegisterSuggester() {
        SearchModule module = new SearchModule(Settings.EMPTY, new NamedWriteableRegistry());
        module.registerSuggester("custom", CustomSuggester.PROTOTYPE);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> module.registerSuggester("custom", CustomSuggester.PROTOTYPE));
        assertEquals("Can't register the same [suggester] more than once for [custom]", e.getMessage());
    }

    public void testRegisterHighlighter() {
        SearchModule module = new SearchModule(Settings.EMPTY, new NamedWriteableRegistry());
        module.registerHighlighter("custom", CustomHighlighter.class);
        try {
            module.registerHighlighter("custom", CustomHighlighter.class);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Can't register the same [highlighter] more than once for [custom]");
        }
        assertMapMultiBinding(module, Highlighter.class, CustomHighlighter.class);
    }

    public void testRegisterQueryParserDuplicate() {
        SearchModule module = new SearchModule(Settings.EMPTY, new NamedWriteableRegistry());
        module.registerQueryParser(TermQueryParser::new);
        try {
            module.buildQueryParserRegistry();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("already registered for name [term] while trying to register [org.elasticsearch.index."));
        }
    }

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

}
