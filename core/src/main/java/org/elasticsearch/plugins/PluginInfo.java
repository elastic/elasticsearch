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
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Properties;

/**
 * An in-memory representation of the plugin descriptor.
 */
public class PluginInfo implements Writeable, ToXContentObject {

    public static final String ES_PLUGIN_PROPERTIES = "plugin-descriptor.properties";
    public static final String ES_PLUGIN_POLICY = "plugin-security.policy";

    private final String name;
    private final String description;
    private final String version;
    private final String classname;
    private final boolean hasNativeController;
    private final boolean requiresKeystore;

    /**
     * Construct plugin info.
     *
     * @param name                the name of the plugin
     * @param description         a description of the plugin
     * @param version             the version of Elasticsearch the plugin is built for
     * @param classname           the entry point to the plugin
     * @param hasNativeController whether or not the plugin has a native controller
     * @param requiresKeystore    whether or not the plugin requires the elasticsearch keystore to be created
     */
    public PluginInfo(String name, String description, String version, String classname,
                      boolean hasNativeController, boolean requiresKeystore) {
        this.name = name;
        this.description = description;
        this.version = version;
        this.classname = classname;
        this.hasNativeController = hasNativeController;
        this.requiresKeystore = requiresKeystore;
    }

    /**
     * Construct plugin info from a stream.
     *
     * @param in the stream
     * @throws IOException if an I/O exception occurred reading the plugin info from the stream
     */
    public PluginInfo(final StreamInput in) throws IOException {
        this.name = in.readString();
        this.description = in.readString();
        this.version = in.readString();
        this.classname = in.readString();
        if (in.getVersion().onOrAfter(Version.V_5_4_0)) {
            hasNativeController = in.readBoolean();
        } else {
            hasNativeController = false;
        }
        if (in.getVersion().onOrAfter(Version.V_6_0_0_beta2)) {
            requiresKeystore = in.readBoolean();
        } else {
            requiresKeystore = false;
        }
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(description);
        out.writeString(version);
        out.writeString(classname);
        if (out.getVersion().onOrAfter(Version.V_5_4_0)) {
            out.writeBoolean(hasNativeController);
        }
        if (out.getVersion().onOrAfter(Version.V_6_0_0_beta2)) {
            out.writeBoolean(requiresKeystore);
        }
    }

    /** reads (and validates) plugin metadata descriptor file */

    /**
     * Reads and validates the plugin descriptor file.
     *
     * @param path the path to the root directory for the plugin
     * @return the plugin info
     * @throws IOException if an I/O exception occurred reading the plugin descriptor
     */
    public static PluginInfo readFromProperties(final Path path) throws IOException {
        final Path descriptor = path.resolve(ES_PLUGIN_PROPERTIES);
        final Properties props = new Properties();
        try (InputStream stream = Files.newInputStream(descriptor)) {
            props.load(stream);
        }
        final String name = props.getProperty("name");
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException(
                    "property [name] is missing in [" + descriptor + "]");
        }
        final String description = props.getProperty("description");
        if (description == null) {
            throw new IllegalArgumentException(
                    "property [description] is missing for plugin [" + name + "]");
        }
        final String version = props.getProperty("version");
        if (version == null) {
            throw new IllegalArgumentException(
                    "property [version] is missing for plugin [" + name + "]");
        }

        final String esVersionString = props.getProperty("elasticsearch.version");
        if (esVersionString == null) {
            throw new IllegalArgumentException(
                    "property [elasticsearch.version] is missing for plugin [" + name + "]");
        }
        final Version esVersion = Version.fromString(esVersionString);
        if (esVersion.equals(Version.CURRENT) == false) {
            final String message = String.format(
                    Locale.ROOT,
                    "plugin [%s] is incompatible with version [%s]; was designed for version [%s]",
                    name,
                    Version.CURRENT.toString(),
                    esVersionString);
            throw new IllegalArgumentException(message);
        }
        final String javaVersionString = props.getProperty("java.version");
        if (javaVersionString == null) {
            throw new IllegalArgumentException(
                    "property [java.version] is missing for plugin [" + name + "]");
        }
        JarHell.checkVersionFormat(javaVersionString);
        JarHell.checkJavaVersion(name, javaVersionString);
        final String classname = props.getProperty("classname");
        if (classname == null) {
            throw new IllegalArgumentException(
                    "property [classname] is missing for plugin [" + name + "]");
        }

        final String hasNativeControllerValue = props.getProperty("has.native.controller");
        final boolean hasNativeController;
        if (hasNativeControllerValue == null) {
            hasNativeController = false;
        } else {
            switch (hasNativeControllerValue) {
                case "true":
                    hasNativeController = true;
                    break;
                case "false":
                    hasNativeController = false;
                    break;
                default:
                    final String message = String.format(
                        Locale.ROOT,
                        "property [%s] must be [%s], [%s], or unspecified but was [%s]",
                        "has_native_controller",
                        "true",
                        "false",
                        hasNativeControllerValue);
                    throw new IllegalArgumentException(message);
            }
        }

        final String requiresKeystoreValue = props.getProperty("requires.keystore", "false");
        final boolean requiresKeystore;
        try {
            requiresKeystore = Booleans.parseBoolean(requiresKeystoreValue);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("property [requires.keystore] must be [true] or [false]," +
                                               " but was [" + requiresKeystoreValue + "]", e);
        }

        return new PluginInfo(name, description, version, classname, hasNativeController, requiresKeystore);
    }

    /**
     * The name of the plugin.
     *
     * @return the plugin name
     */
    public String getName() {
        return name;
    }

    /**
     * The description of the plugin.
     *
     * @return the plugin description
     */
    public String getDescription() {
        return description;
    }

    /**
     * The entry point to the plugin.
     *
     * @return the entry point to the plugin
     */
    public String getClassname() {
        return classname;
    }

    /**
     * The version of Elasticsearch the plugin was built for.
     *
     * @return the version
     */
    public String getVersion() {
        return version;
    }

    /**
     * Whether or not the plugin has a native controller.
     *
     * @return {@code true} if the plugin has a native controller
     */
    public boolean hasNativeController() {
        return hasNativeController;
    }

    /**
     * Whether or not the plugin requires the elasticsearch keystore to exist.
     *
     * @return {@code true} if the plugin requires a keystore, {@code false} otherwise
     */
    public boolean requiresKeystore() {
        return requiresKeystore;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("name", name);
            builder.field("version", version);
            builder.field("description", description);
            builder.field("classname", classname);
            builder.field("has_native_controller", hasNativeController);
            builder.field("requires_keystore", requiresKeystore);
        }
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
                .append("Native Controller: ").append(hasNativeController).append("\n")
                .append("Requires Keystore: ").append(requiresKeystore).append("\n")
                .append(" * Classname: ").append(classname);
        return information.toString();
    }

}
