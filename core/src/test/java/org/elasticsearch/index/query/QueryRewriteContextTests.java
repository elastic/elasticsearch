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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.ScriptSettings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;

import static java.util.Collections.emptyList;

public class QueryRewriteContextTests extends ESTestCase {

    public void testNewParseContextWithLegacyScriptLanguage() throws Exception {
        String defaultLegacyScriptLanguage = randomAsciiOfLength(4);
        IndexMetaData.Builder indexMetadata = new IndexMetaData.Builder("index");
        indexMetadata.settings(Settings.builder().put("index.version.created", Version.CURRENT)
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1)
        );
        IndicesQueriesRegistry indicesQueriesRegistry = new SearchModule(Settings.EMPTY, false, emptyList()).getQueryParserRegistry();
        IndexSettings indexSettings = new IndexSettings(indexMetadata.build(),
                Settings.builder().put(ScriptSettings.LEGACY_SCRIPT_SETTING, defaultLegacyScriptLanguage).build());
        QueryRewriteContext queryRewriteContext =
                new QueryRewriteContext(indexSettings, null, null, indicesQueriesRegistry, null, null, null);;

        // verify that the default script language in the query parse context is equal to defaultLegacyScriptLanguage variable:
        QueryParseContext queryParseContext =
                queryRewriteContext.newParseContextWithLegacyScriptLanguage(XContentHelper.createParser(new BytesArray("{}")));
        assertEquals(defaultLegacyScriptLanguage, queryParseContext.getDefaultScriptLanguage());

        // verify that the script query's script language is equal to defaultLegacyScriptLanguage variable:
        XContentParser parser = XContentHelper.createParser(new BytesArray("{\"script\" : {\"script\": \"return true\"}}"));
        queryParseContext = queryRewriteContext.newParseContextWithLegacyScriptLanguage(parser);
        ScriptQueryBuilder queryBuilder = (ScriptQueryBuilder) queryParseContext.parseInnerQueryBuilder().get();
        assertEquals(defaultLegacyScriptLanguage, queryBuilder.script().getLang());
    }

}
