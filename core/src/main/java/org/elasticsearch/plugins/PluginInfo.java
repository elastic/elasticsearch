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
    public static final String ES_PLUGIN_POLICY = "plugin-security.policy";

    static final class Fields {
        static final XContentBuilderString NAME = new XContentBuilderString("name");
        static final XContentBuilderString DESCRIPTION = new XContentBuilderString("description");
        static final XContentBuilderString URL = new XContentBuilderString("url");
        static final XContentBuilderString VERSION = new XContentBuilderString("version");
        static final XContentBuilderString CLASSNAME = new XContentBuilderString("classname");
        static final XContentBuilderString ISOLATED = new XContentBuilderString("isolated");
    }

    private String name;
    private String description;
    private String version;
    private String classname;
    private boolean isolated;

    public PluginInfo() {
    }

    /**
     * Information about plugins
     *
     * @param name        Its name
     * @param description Its description
     * @param version     Version number
     */
    PluginInfo(String name, String description, String version, String classname, boolean isolated) {
        this.name = name;
        this.description = description;
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

        String esVersionString = props.getProperty("elasticsearch.version");
        if (esVersionString == null) {
            throw new IllegalArgumentException("Property [elasticsearch.version] is missing for plugin [" + name + "]");
        }
        Version esVersion = Version.fromString(esVersionString);
        if (esVersion.equals(Version.CURRENT) == false) {
            throw new IllegalArgumentException("Plugin [" + name + "] is incompatible with Elasticsearch [" + Version.CURRENT.toString() +
                    "]. Was designed for version [" + esVersionString + "]");
        }
        String javaVersionString = props.getProperty("java.version");
        if (javaVersionString == null) {
            throw new IllegalArgumentException("Property [java.version] is missing for plugin [" + name + "]");
        }
        JarHell.checkVersionFormat(javaVersionString);
        JarHell.checkJavaVersion(name, javaVersionString);
        boolean isolated = Boolean.parseBoolean(props.getProperty("isolated", "true"));
        String classname = props.getProperty("classname");
        if (classname == null) {
            throw new IllegalArgumentException("Property [classname] is missing for plugin [" + name + "]");
        }

        return new PluginInfo(name, description, version, classname, isolated);
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
     * @return true if plugin has isolated classloader
     */
    public boolean isIsolated() {
        return isolated;
    }

    /**
     * @return plugin's classname
     */
    public String getClassname() {
        return classname;
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
        this.version = in.readString();
        this.classname = in.readString();
        this.isolated = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(description);
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
        builder.field(Fields.CLASSNAME, classname);
        builder.field(Fields.ISOLATED, isolated);
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
                .append("Version: ").append(version).append("\n")
                .append(" * Classname: ").append(classname).append("\n")
                .append(" * Isolated: ").append(isolated);

        return information.toString();
    }
}
