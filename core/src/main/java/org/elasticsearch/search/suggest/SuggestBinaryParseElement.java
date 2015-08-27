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
package org.elasticsearch.search.suggest;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public final class SuggestBinaryParseElement extends SuggestParseElement {

    @Inject
    public SuggestBinaryParseElement(Suggesters suggesters) {
        super(suggesters);
    }

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        byte[] suggestSource = parser.binaryValue();
        assert suggestSource.length > 0;
        try (XContentParser suggestSourceParser = XContentFactory.xContent(suggestSource).createParser(suggestSource)) {
            suggestSourceParser.nextToken(); // move past the first START_OBJECT
            super.parse(suggestSourceParser, context);
        }
    }
}
