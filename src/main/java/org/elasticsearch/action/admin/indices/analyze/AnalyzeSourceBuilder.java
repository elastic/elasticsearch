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

package org.elasticsearch.action.admin.indices.analyze;

import com.google.common.collect.Lists;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.io.IOException;
import java.util.List;

/**
 * Builder to create the analyze rquest body.
 */
public class AnalyzeSourceBuilder implements ToXContent {

    private String text;
    private String analyzer;
    private String tokenizer;
    private List<String> tokenFilters;
    private List<String> charFilters;
    private String field;
    private boolean preferLocal;

    public AnalyzeSourceBuilder setText(String text) {
        this.text = text;
        return this;
    }

    public AnalyzeSourceBuilder setAnalyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public AnalyzeSourceBuilder setTokenizer(String tokenizer) {
        this.tokenizer = tokenizer;
        return this;
    }

    public AnalyzeSourceBuilder setTokenFilters(String... tokenFilters) {
        if (tokenFilters != null) {
            if (this.tokenFilters == null) {
                this.tokenFilters = Lists.newArrayList(tokenFilters);
            }
        }
        return this;
    }

    public AnalyzeSourceBuilder setTokenFilters(List<String> tokenFilters) {
        this.tokenFilters = tokenFilters;
        return this;
    }

    public AnalyzeSourceBuilder setCharFilters(List<String> charFilters) {
        this.charFilters = charFilters;
        return this;
    }

    public AnalyzeSourceBuilder setCharFilters(String... charFilters) {
        if (charFilters != null) {
            if (this.charFilters == null) {
                this.charFilters = Lists.newArrayList(charFilters);
            }
        }
        return this;
    }

    public AnalyzeSourceBuilder setField(String field) {
        this.field = field;
        return this;
    }

    public AnalyzeSourceBuilder setPreferLocal(boolean preferLocal) {
        this.preferLocal = preferLocal;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (text != null) {
            builder.field("text", text);
        }
        if (analyzer != null) {
            builder.field("analyzer", analyzer);
        }
        if (tokenizer != null) {
            builder.field("tokenizer", tokenizer);
        }
        if (tokenFilters != null) {
            builder.startArray("filters");
            for (String tokenFilter : tokenFilters) {
                builder.value(tokenFilter);
            }
            builder.endArray();
        }
        if (charFilters != null) {
            builder.startArray("char_filters");
            for (String charFilter : charFilters) {
                builder.value(charFilter);
            }
            builder.endArray();
        }
        if (field != null) {
            builder.field("field", field);
        }
        builder.field("prefer_local", preferLocal);
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
