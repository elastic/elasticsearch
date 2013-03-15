package org.elasticsearch.action.admin.cluster.node.info;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.io.Serializable;

public class PluginInfo implements Streamable, Serializable, ToXContent {
    static final class Fields {
        static final XContentBuilderString NAME = new XContentBuilderString("name");
        static final XContentBuilderString DESCRIPTION = new XContentBuilderString("description");
        static final XContentBuilderString URL = new XContentBuilderString("url");
        static final XContentBuilderString JVM = new XContentBuilderString("jvm");
        static final XContentBuilderString SITE = new XContentBuilderString("site");
    }

    private String name;
    private String description;
    private boolean site;
    private boolean jvm;

    public PluginInfo() {
    }

    /**
     * Information about plugins
     * @param name Its name
     * @param description Its description
     * @param site true if it's a site plugin
     * @param jvm true if it's a jvm plugin
     */
    public PluginInfo(String name, String description, boolean site, boolean jvm) {
        this.name = name;
        this.description = description;
        this.site = site;
        this.jvm = jvm;
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
     * @return true is it's a site plugin
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
     * @return
     */
    public String getUrl() {
        if (site) {
            return ("/_plugin/" + name + "/");
        } else {
            return null;
        }
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
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(description);
        out.writeBoolean(site);
        out.writeBoolean(jvm);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Fields.NAME, name);
        builder.field(Fields.DESCRIPTION, description);
        if (site) {
            builder.field(Fields.URL, getUrl());
        }
        builder.field(Fields.JVM, jvm);
        builder.field(Fields.SITE, site);
        builder.endObject();

        return builder;
    }
}
