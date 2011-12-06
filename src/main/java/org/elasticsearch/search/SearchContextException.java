/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.search.internal.SearchContext;

/**
 *
 */
public class SearchContextException extends SearchException {

    public SearchContextException(SearchContext context, String msg) {
        super(context.shardTarget(), buildMessage(context, msg));
    }

    public SearchContextException(SearchContext context, String msg, Throwable t) {
        super(context.shardTarget(), buildMessage(context, msg), t);
    }

    private static String buildMessage(SearchContext context, String msg) {
        StringBuilder sb = new StringBuilder();
        sb.append('[').append(context.shardTarget().index()).append("][").append(context.shardTarget().shardId()).append("]: ");
        if (context.parsedQuery() != null) {
            try {
                sb.append("query[").append(context.parsedQuery().query()).append("],");
            } catch (Exception e) {
                sb.append("query[_failed_to_string_],");
            }
        }
        sb.append("from[").append(context.from()).append("],size[").append(context.size()).append("]");
        if (context.sort() != null) {
            sb.append(",sort[").append(context.sort()).append("]");
        }
        return sb.append(": ").append(msg).toString();
    }
}
