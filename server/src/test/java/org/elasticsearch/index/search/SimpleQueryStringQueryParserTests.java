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

package org.elasticsearch.index.search;

import org.elasticsearch.test.ESTestCase;

public class SimpleQueryStringQueryParserTests extends ESTestCase {

    public void testEqualsSettings() {
        SimpleQueryStringQueryParser.Settings settings1 = new SimpleQueryStringQueryParser.Settings();
        SimpleQueryStringQueryParser.Settings settings2 = new SimpleQueryStringQueryParser.Settings();
        String s = "Some random other object";
        assertEquals(settings1, settings1);
        assertEquals(settings1, settings2);
        assertNotEquals(settings1, null);
        assertNotEquals(settings1, s);

        settings2.lenient(!settings1.lenient());
        assertNotEquals(settings1, settings2);

        settings2 = new SimpleQueryStringQueryParser.Settings();
        settings2.analyzeWildcard(!settings1.analyzeWildcard());
        assertNotEquals(settings1, settings2);

        settings2 = new SimpleQueryStringQueryParser.Settings();
        settings2.quoteFieldSuffix("a");
        assertNotEquals(settings1, settings2);

        settings2 = new SimpleQueryStringQueryParser.Settings();
        settings2.autoGenerateSynonymsPhraseQuery(!settings1.autoGenerateSynonymsPhraseQuery());
        assertNotEquals(settings1, settings2);

        settings2 = new SimpleQueryStringQueryParser.Settings();
        settings2.fuzzyPrefixLength(settings1.fuzzyPrefixLength() + 1);
        assertNotEquals(settings1, settings2);

        settings2 = new SimpleQueryStringQueryParser.Settings();
        settings2.fuzzyMaxExpansions(settings1.fuzzyMaxExpansions() + 1);
        assertNotEquals(settings1, settings2);

        settings2 = new SimpleQueryStringQueryParser.Settings();
        settings2.fuzzyTranspositions(!settings1.fuzzyTranspositions());
        assertNotEquals(settings1, settings2);
    }
}
