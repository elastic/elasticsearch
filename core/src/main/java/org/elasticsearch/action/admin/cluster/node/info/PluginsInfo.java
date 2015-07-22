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
import org.elasticsearch.plugins.PluginInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class PluginsInfo implements Streamable, ToXContent {
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

    /**
     * @return an ordered list based on plugins name
     */
    public List<PluginInfo> getInfos() {
        Collections.sort(infos, new Comparator<PluginInfo>() {
            @Override
            public int compare(final PluginInfo o1, final PluginInfo o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });

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
            infos.add(PluginInfo.readFromStream(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(infos.size());
        for (PluginInfo plugin : getInfos()) {
            plugin.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(Fields.PLUGINS);
        for (PluginInfo pluginInfo : getInfos()) {
            pluginInfo.toXContent(builder, params);
        }
        builder.endArray();

        return builder;
    }
}
