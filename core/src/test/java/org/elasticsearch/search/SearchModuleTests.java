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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.highlight.CustomHighlighter;
import org.elasticsearch.search.highlight.Highlighter;
import org.elasticsearch.search.highlight.PlainHighlighter;
import org.elasticsearch.search.suggest.CustomSuggester;
import org.elasticsearch.search.suggest.Suggester;
import org.elasticsearch.search.suggest.phrase.PhraseSuggester;
/**
 */
public class SearchModuleTests extends ModuleTestCase {

   public void testDoubleRegister() {
       SearchModule module = new SearchModule(Settings.EMPTY);
       try {
           module.registerHighlighter("fvh", PlainHighlighter.class);
       } catch (IllegalArgumentException e) {
           assertEquals(e.getMessage(), "Can't register the same [highlighter] more than once for [fvh]");
       }

       try {
           module.registerSuggester("term", PhraseSuggester.class);
       } catch (IllegalArgumentException e) {
           assertEquals(e.getMessage(), "Can't register the same [suggester] more than once for [term]");
       }
   }

    public void testRegisterSuggester() {
        SearchModule module = new SearchModule(Settings.EMPTY);
        module.registerSuggester("custom", CustomSuggester.class);
        try {
            module.registerSuggester("custom", CustomSuggester.class);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Can't register the same [suggester] more than once for [custom]");
        }
        assertMapMultiBinding(module, Suggester.class, CustomSuggester.class);
    }

    public void testRegisterHighlighter() {
        SearchModule module = new SearchModule(Settings.EMPTY);
        module.registerHighlighter("custom", CustomHighlighter.class);
        try {
            module.registerHighlighter("custom", CustomHighlighter.class);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Can't register the same [highlighter] more than once for [custom]");
        }
        assertMapMultiBinding(module, Highlighter.class, CustomHighlighter.class);
    }
}
