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

import java.io.IOException;

/**
 * Class used during the query parsers refactoring. Will be removed once we can parse search requests on the coordinating node.
 * All query parsers that have a refactored "fromXContent" method can be changed to extend this instead of {@link BaseQueryParserTemp}.
 * Keeps old {@link QueryParser#parse(QueryShardContext)} method as a stub delegating to
 * {@link QueryParser#fromXContent(QueryShardContext)} and {@link QueryBuilder#toQuery(QueryShardContext)}}
 */
//norelease needs to be removed once we parse search requests on the coordinating node, as the parse method is not needed anymore at that point.
public abstract class BaseQueryParser implements QueryParser {

    @Override
    public final Query parse(QueryShardContext context) throws IOException, QueryParsingException {
        return fromXContent(context.parseContext()).toQuery(context);
    }
}
