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

package org.elasticsearch.action.search;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.io.IOException;

/**
 * Builder to create the search scroll builder
 */
public class SearchScrollSourceBuilder implements ToXContent{

    private String scrollId;
    private String scroll;

    public SearchScrollSourceBuilder scrollId(String scrollId) {
        this.scrollId = scrollId;
        return this;
    }

    /**
     * If set, will enable scrolling of the search request.
     */
    public SearchScrollSourceBuilder scroll(Scroll scroll) {
        if (scroll != null) {
            this.scroll = scroll.keepAlive().toString();
        }
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public SearchScrollSourceBuilder scroll(TimeValue keepAlive) {
        if (keepAlive != null) {
            this.scroll = keepAlive.toString();
        }
        return this;
    }

    /**
     * If set, will enable scrolling of the search request for the specified timeout.
     */
    public SearchScrollSourceBuilder scroll(String keepAlive) {
        this.scroll = keepAlive;
        return this;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("scroll_id", this.scrollId);
        if (scroll != null) {
            builder.field("scroll", this.scroll);
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
