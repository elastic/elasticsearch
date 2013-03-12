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

package org.elasticsearch.plugins;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Set;

/**
 *
 */
public class PluginsInfo implements Streamable, Serializable, ToXContent {

    private static final String[] EMPTY = new String[0];

    private String[] mandatoryPlugins;
    private String[] sitePlugins;
    private String[] nonsitePlugins;

    public PluginsInfo() {
    }

    public PluginsInfo(@Nullable Set<String> mandatoryPlugins, Set<String> sitePlugins, Set<String> nonsitePlugins) {
        this.mandatoryPlugins = mandatoryPlugins == null ? EMPTY :
                mandatoryPlugins.toArray(new String[mandatoryPlugins.size()]);
        this.sitePlugins = sitePlugins.toArray(new String[sitePlugins.size()]);
        this.nonsitePlugins = nonsitePlugins.toArray(new String[nonsitePlugins.size()]);
        Arrays.sort(this.mandatoryPlugins);
        Arrays.sort(this.sitePlugins);
        Arrays.sort(this.nonsitePlugins);
    }

    static final class Fields {
        static final XContentBuilderString PLUGINS = new XContentBuilderString("plugins");
        static final XContentBuilderString MANDATORY = new XContentBuilderString("mandatory");
        static final XContentBuilderString SITES = new XContentBuilderString("sites");
        static final XContentBuilderString NONSITES = new XContentBuilderString("nonsites");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.PLUGINS);
        builder.array(Fields.MANDATORY, mandatoryPlugins);
        builder.array(Fields.SITES, sitePlugins);
        builder.array(Fields.NONSITES, nonsitePlugins);
        builder.endObject();
        return builder;
    }

    public static PluginsInfo readPluginsInfo(StreamInput in) throws IOException {
        PluginsInfo info = new PluginsInfo();
        info.readFrom(in);
        return info;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        mandatoryPlugins = in.readStringArray();
        sitePlugins = in.readStringArray();
        nonsitePlugins = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(mandatoryPlugins);
        out.writeStringArray(sitePlugins);
        out.writeStringArray(nonsitePlugins);
    }

}
