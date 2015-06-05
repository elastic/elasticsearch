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

package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class PluginsInfo implements Streamable, Serializable, ToXContent {
    static final class Fields {
        static final XContentBuilderString PLUGINS = new XContentBuilderString("plugins");
    }

    private List<PluginInfo> infos;

    public PluginsInfo() {
        infos = new ArrayList<>();
    }

    public PluginsInfo(int size) {
        infos = new ArrayList<>(size);
    }

    public List<PluginInfo> getInfos() {
        return infos;
    }

    public void add(PluginInfo info) {
        infos.add(info);
    }

    public static PluginsInfo readPluginsInfo(StreamInput in) throws IOException {
        PluginsInfo infos = new PluginsInfo();
        infos.readFrom(in);
        return infos;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int plugins_size = in.readInt();
        for (int i = 0; i < plugins_size; i++) {
            infos.add(PluginInfo.readPluginInfo(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(infos.size());
        for (PluginInfo plugin : infos) {
            plugin.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(Fields.PLUGINS);
        for (PluginInfo pluginInfo : infos) {
            pluginInfo.toXContent(builder, params);
        }
        builder.endArray();

        return builder;
    }
}
