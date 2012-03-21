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

package org.elasticsearch.test.unit.index.mapper.source;

import org.elasticsearch.common.BytesHolder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MergeContext;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.source.SourceProvider;
import org.elasticsearch.index.source.SourceProviderParser;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class TestSourceProviderParser implements SourceProviderParser {

    @Override public SourceProvider parse(Map<String, Object> node) {
        return new SourceProvider() {
            @Override public BytesHolder dehydrateSource(ParseContext context) {
                byte[] dehydratedSource = ("id:" + context.id()).getBytes();
                return new BytesHolder(dehydratedSource);
            }

            @Override public String name() {
                return "test";
            }

            @Override public BytesHolder rehydrateSource(String type, String id, byte[] source, int sourceOffset, int sourceLength) {
                StringBuilder builder = new StringBuilder();
                builder.append("--");
                builder.append(new String(source, sourceOffset, sourceLength));
                builder.append("--");
                return new BytesHolder(builder.toString().getBytes());
            }

            @Override public void merge(SourceProvider mergeWith, MergeContext mergeContext) {
            }

            @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return builder;
            }
        };
    }
}