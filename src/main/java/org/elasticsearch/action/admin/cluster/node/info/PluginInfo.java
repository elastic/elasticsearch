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

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.io.Serializable;

public class PluginInfo implements Streamable, Serializable, ToXContent {
    public static final String DESCRIPTION_NOT_AVAILABLE = "No description found.";
    public static final String VERSION_NOT_AVAILABLE = "NA";

    static final class Fields {
        static final XContentBuilderString NAME = new XContentBuilderString("name");
        static final XContentBuilderString DESCRIPTION = new XContentBuilderString("description");
        static final XContentBuilderString URL = new XContentBuilderString("url");
        static final XContentBuilderString JVM = new XContentBuilderString("jvm");
        static final XContentBuilderString SITE = new XContentBuilderString("site");
        static final XContentBuilderString VERSION = new XContentBuilderString("version");
    }

    private String name;
    private String description;
    private boolean site;
    private boolean jvm;
    private String version;

    public PluginInfo() {
    }

    /**
     * Information about plugins
     *
     * @param name        Its name
     * @param description Its description
     * @param site        true if it's a site plugin
     * @param jvm         true if it's a jvm plugin
     * @param version     Version number is applicable (NA otherwise)
     */
    public PluginInfo(String name, String description, boolean site, boolean jvm, String version) {
        this.name = name;
        this.description = description;
        this.site = site;
        this.jvm = jvm;
        if (Strings.hasText(version)) {
            this.version = version;
        } else {
            this.version = VERSION_NOT_AVAILABLE;
        }
    }

    /**
     * @return Plugin's name
     */
    public String getName() {
        return name;
    }

    /**
     * @return Plugin's description if any
     */
    public String getDescription() {
        return description;
    }

    /**
     * @return true if it's a site plugin
     */
    public boolean isSite() {
        return site;
    }

    /**
     * @return true if it's a plugin running in the jvm
     */
    public boolean isJvm() {
        return jvm;
    }

    /**
     * We compute the URL for sites: "/_plugin/" + name + "/"
     *
     * @return relative URL for site plugin
     */
    public String getUrl() {
        if (site) {
            return ("/_plugin/" + name + "/");
        } else {
            return null;
        }
    }

    /**
     * @return Version number for the plugin
     */
    public String getVersion() {
        return version;
    }

    public static PluginInfo readPluginInfo(StreamInput in) throws IOException {
        PluginInfo info = new PluginInfo();
        info.readFrom(in);
        return info;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.name = in.readString();
        this.description = in.readString();
        this.site = in.readBoolean();
        this.jvm = in.readBoolean();
        this.version = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(description);
        out.writeBoolean(site);
        out.writeBoolean(jvm);
        out.writeString(version);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Fields.NAME, name);
        builder.field(Fields.VERSION, version);
        builder.field(Fields.DESCRIPTION, description);
        if (site) {
            builder.field(Fields.URL, getUrl());
        }
        builder.field(Fields.JVM, jvm);
        builder.field(Fields.SITE, site);
        builder.endObject();

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PluginInfo that = (PluginInfo) o;

        if (!name.equals(that.name)) return false;
        if (version != null ? !version.equals(that.version) : that.version != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("PluginInfo{");
        sb.append("name='").append(name).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", site=").append(site);
        sb.append(", jvm=").append(jvm);
        sb.append(", version='").append(version).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
