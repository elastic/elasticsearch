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

package org.elasticsearch.search.fetch.innerhits;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.support.BaseInnerHitBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class InnerHitsBuilder implements ToXContent {

    private Map<String, InnerHit> innerHits = new HashMap<>();

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("inner_hits");
        for (Map.Entry<String, InnerHit> entry : innerHits.entrySet()) {
            builder.startObject(entry.getKey());
            entry.getValue().toXContent(builder, params);
            builder.endObject();
        }
        return builder.endObject();
    }

    public void addInnerHit(String name, InnerHit innerHit) {
        innerHits.put(name, innerHit);
    }

    public static class InnerHit extends BaseInnerHitBuilder<InnerHit> {

        private String path;
        private String type;

        /**
         * Sets the query to run for collecting the inner hits.
         */
        public InnerHit setQuery(QueryBuilder query) {
            sourceBuilder().query(query);
            return this;
        }

        /**
         * For parent/child inner hits the type to collect inner hits for.
         */
        public InnerHit setPath(String path) {
            this.path = path;
            return this;
        }

        /**
         * For nested inner hits the path to collect child nested docs for.
         */
        public InnerHit setType(String type) {
            this.type = type;
            return this;
        }

        /**
         * Adds a nested inner hit definition that collects inner hits for hits
         * on this inner hit level.
         */
        public InnerHit addInnerHit(String name, InnerHit innerHit) {
            sourceBuilder().innerHitsBuilder().addInnerHit(name, innerHit);
            return this;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (path != null) {
                builder.startObject("path").startObject(path);
            } else {
                builder.startObject("type").startObject(type);
            }
            super.toXContent(builder, params);
            return builder.endObject().endObject();
        }
    }

}
