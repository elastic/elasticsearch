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

import org.elasticsearch.Version;
import org.elasticsearch.bootstrap.JarHell;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class PluginInfo implements Streamable, ToXContent {

    public static final String ES_PLUGIN_PROPERTIES = "plugin-descriptor.properties";

    static final class Fields {
        static final XContentBuilderString NAME = new XContentBuilderString("name");
        static final XContentBuilderString DESCRIPTION = new XContentBuilderString("description");
        static final XContentBuilderString URL = new XContentBuilderString("url");
        static final XContentBuilderString SITE = new XContentBuilderString("site");
        static final XContentBuilderString VERSION = new XContentBuilderString("version");
        static final XContentBuilderString JVM = new XContentBuilderString("jvm");
        static final XContentBuilderString CLASSNAME = new XContentBuilderString("classname");
        static final XContentBuilderString ISOLATED = new XContentBuilderString("isolated");
    }

    private String name;
    private String description;
    private boolean site;
    private String version;

    private boolean jvm;
    private String classname;
    private boolean isolated;

    public PluginInfo() {
    }

    /**
     * Information about plugins
     *
     * @param name        Its name
     * @param description Its description
     * @param site        true if it's a site plugin
     * @param jvm         true if it's a jvm plugin
     * @param version     Version number
     */
    PluginInfo(String name, String description, boolean site, String version, boolean jvm, String classname, boolean isolated) {
        this.name = name;
        this.description = description;
        this.site = site;
        this.jvm = jvm;
        this.version = version;
        this.classname = classname;
        this.isolated = isolated;
    }

    /** reads (and validates) plugin metadata descriptor file */
    public static PluginInfo readFromProperties(Path dir) throws IOException {
        Path descriptor = dir.resolve(ES_PLUGIN_PROPERTIES);
        Properties props = new Properties();
        try (InputStream stream = Files.newInputStream(descriptor)) {
            props.load(stream);
        }
        String name = props.getProperty("name");
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Property [name] is missing in [" + descriptor + "]");
        }
        PluginManager.checkForForbiddenName(name);
        String description = props.getProperty("description");
        if (description == null) {
            throw new IllegalArgumentException("Property [description] is missing for plugin [" + name + "]");
        }
        String version = props.getProperty("version");
        if (version == null) {
            throw new IllegalArgumentException("Property [version] is missing for plugin [" + name + "]");
        }

        boolean jvm = Boolean.parseBoolean(props.getProperty("jvm"));
        boolean site = Boolean.parseBoolean(props.getProperty("site"));
        if (jvm == false && site == false) {
            throw new IllegalArgumentException("Plugin [" + name + "] must be at least a jvm or site plugin");
        }
        boolean isolated = true;
        String classname = "NA";
        if (jvm) {
            String esVersionString = props.getProperty("elasticsearch.version");
            if (esVersionString == null) {
                throw new IllegalArgumentException("Property [elasticsearch.version] is missing for jvm plugin [" + name + "]");
            }
            Version esVersion = Version.fromString(esVersionString);
            if (esVersion.equals(Version.CURRENT) == false) {
                throw new IllegalArgumentException("Elasticsearch version [" + esVersionString + "] is too old for plugin [" + name + "]");
            }
            String javaVersionString = props.getProperty("java.version");
            if (javaVersionString == null) {
                throw new IllegalArgumentException("Property [java.version] is missing for jvm plugin [" + name + "]");
            }
            JarHell.checkVersionFormat(javaVersionString);
            JarHell.checkJavaVersion(name, javaVersionString);
            isolated = Boolean.parseBoolean(props.getProperty("isolated", "true"));
            classname = props.getProperty("classname");
            if (classname == null) {
                throw new IllegalArgumentException("Property [classname] is missing for jvm plugin [" + name + "]");
            }
        }

        if (site) {
            if (!Files.exists(dir.resolve("_site"))) {
                throw new IllegalArgumentException("Plugin [" + name + "] is a site plugin but has no '_site/' directory");
            }
        }

        return new PluginInfo(name, description, site, version, jvm, classname, isolated);
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
     * @return true if jvm plugin has isolated classloader
     */
    public boolean isIsolated() {
        return isolated;
    }

    /**
     * @return jvm plugin's classname
     */
    public String getClassname() {
        return classname;
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

    public static PluginInfo readFromStream(StreamInput in) throws IOException {
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
        this.classname = in.readString();
        this.isolated = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(description);
        out.writeBoolean(site);
        out.writeBoolean(jvm);
        out.writeString(version);
        out.writeString(classname);
        out.writeBoolean(isolated);
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
        if (jvm) {
            builder.field(Fields.CLASSNAME, classname);
            builder.field(Fields.ISOLATED, isolated);
        }
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
        final StringBuilder information = new StringBuilder()
                .append("- Plugin information:\n")
                .append("Name: ").append(name).append("\n")
                .append("Description: ").append(description).append("\n")
                .append("Site: ").append(site).append("\n")
                .append("Version: ").append(version).append("\n")
                .append("JVM: ").append(jvm).append("\n");

        if (jvm) {
            information.append(" * Classname: ").append(classname).append("\n");
            information.append(" * Isolated: ").append(isolated);
        }

        return information.toString();
    }
}
