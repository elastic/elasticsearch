/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.updatebyquery;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.io.IOException;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 * Source builder of the script, lang, params and query for a update by query request.
 */
public class UpdateByQuerySourceBuilder implements ToXContent {

    private QueryBuilder queryBuilder;
    private BytesReference queryBinary;
    private String script;
    private String scriptLang;
    private Map<String, Object> scriptParams = newHashMap();

    public UpdateByQuerySourceBuilder query(QueryBuilder query) {
        this.queryBuilder = query;
        return this;
    }

    public UpdateByQuerySourceBuilder query(BytesReference queryBinary) {
        this.queryBinary = queryBinary;
        return this;
    }

    public UpdateByQuerySourceBuilder script(String script) {
        this.script = script;
        return this;
    }

    public UpdateByQuerySourceBuilder scriptLang(String scriptLang) {
        this.scriptLang = scriptLang;
        return this;
    }

    public UpdateByQuerySourceBuilder scriptParams(Map<String, Object> scriptParams) {
        this.scriptParams = scriptParams;
        return this;
    }

    public UpdateByQuerySourceBuilder addScriptParam(String name, String value) {
        scriptParams.put(name, value);
        return this;
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (queryBuilder != null) {
            builder.field("query");
            queryBuilder.toXContent(builder, params);
        }

        if (queryBinary != null) {
            if (XContentFactory.xContentType(queryBinary) == builder.contentType()) {
                builder.rawField("query", queryBinary);
            } else {
                builder.field("query_binary", queryBinary);
            }
        }

        if (script != null) {
            builder.field("script", script);
        }

        if (scriptLang != null) {
            builder.field("lang", scriptLang);
        }

        if (!scriptParams.isEmpty()) {
            builder.field("params", scriptParams);
        }

        builder.endObject();
        return builder;
    }

    public BytesReference buildAsBytes(XContentType contentType) throws SearchSourceBuilderException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            toXContent(builder, ToXContent.EMPTY_PARAMS);
            return builder.bytes();
        } catch (Exception e) {
            throw new SearchSourceBuilderException("Failed to build search source", e);
        }
    }
}
