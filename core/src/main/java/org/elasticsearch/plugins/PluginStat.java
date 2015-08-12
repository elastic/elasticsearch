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

package org.elasticsearch.plugins;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class PluginStat implements Streamable, ToXContent {

    private String category;
    private CompressedXContent data;

    public PluginStat() {
    }

    public PluginStat(String category, CompressedXContent data) {
        this.category = category;
        this.data = data;
    }

    public PluginStat(String category, BytesReference data) throws IOException {
        this(category, new CompressedXContent(data));
    }

    public PluginStat(String category, ToXContent xcontent, XContentType type, ToXContent.Params params) throws IOException {
        this(category, new CompressedXContent(xcontent, type, params));
    }

    public Map<String, Object> getStatsAsMap() throws IOException {
        Tuple<XContentType, Map<String, Object>> statAsMap = XContentHelper.convertToMap(data.compressedReference(), true);

        Map<String, Object> statsAsMap = new HashMap<>();
        statsAsMap.put(category, statAsMap.v2());
        return statsAsMap;
    }

    public String getCategory() {
        return category;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        category = in.readOptionalString();
        data = CompressedXContent.readCompressedString(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(category);
        data.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        try (XContentParser parser = XContentHelper.createParser(data.compressedReference())) {
            XContentHelper.copyCurrentStructure(builder.generator(), parser);
        }
        return builder;
    }

    public static PluginStat readFromStream(StreamInput in) throws IOException {
        PluginStat pluginStat = new PluginStat();
        pluginStat.readFrom(in);
        return pluginStat;
    }
}