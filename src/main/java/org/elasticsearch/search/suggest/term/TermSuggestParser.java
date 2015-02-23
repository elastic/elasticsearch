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
package org.elasticsearch.search.suggest.term;

import java.io.IOException;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.suggest.DirectSpellcheckerSettings;
import org.elasticsearch.search.suggest.SuggestContextParser;
import org.elasticsearch.search.suggest.SuggestUtils;
import org.elasticsearch.search.suggest.SuggestionSearchContext;

public final class TermSuggestParser implements SuggestContextParser {

    private TermSuggester suggester;

    public TermSuggestParser(TermSuggester suggester) {
        this.suggester = suggester;
    }

    @Override
    public SuggestionSearchContext.SuggestionContext parse(XContentParser parser, MapperService mapperService) throws IOException {
        XContentParser.Token token;
        String fieldName = null;
        TermSuggestionContext suggestion = new TermSuggestionContext(suggester);
        DirectSpellcheckerSettings settings = suggestion.getDirectSpellCheckerSettings();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                parseTokenValue(parser, mapperService, fieldName, suggestion, settings);
            } else {
                throw new ElasticsearchIllegalArgumentException("suggester[term]  doesn't support field [" + fieldName + "]");
            }
        }
        return suggestion;
    }

    private void parseTokenValue(XContentParser parser, MapperService mapperService, String fieldName, TermSuggestionContext suggestion,
            DirectSpellcheckerSettings settings) throws IOException {
        if (!(SuggestUtils.parseSuggestContext(parser, mapperService, fieldName, suggestion) || SuggestUtils.parseDirectSpellcheckerSettings(
                parser, fieldName, settings))) {
            throw new ElasticsearchIllegalArgumentException("suggester[term] doesn't support [" + fieldName + "]");

        }
    }

}
