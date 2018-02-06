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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    private final List<String> extendedPlugins;
    private final boolean hasNativeController;
    private final boolean requiresKeystore;

    /**
     * Construct plugin info.
     *
     * @param name                the name of the plugin
     * @param description         a description of the plugin
     * @param version             the version of Elasticsearch the plugin is built for
     * @param classname           the entry point to the plugin
     * @param extendedPlugins     other plugins this plugin extends through SPI
     * @param hasNativeController whether or not the plugin has a native controller
     * @param requiresKeystore    whether or not the plugin requires the elasticsearch keystore to be created
     */
    public PluginInfo(String name, String description, String version, String classname,
                      List<String> extendedPlugins, boolean hasNativeController, boolean requiresKeystore) {
        this.name = name;
        this.description = description;
        this.version = version;
        this.classname = classname;
        this.extendedPlugins = Collections.unmodifiableList(extendedPlugins);
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
        if (in.getVersion().onOrAfter(Version.V_6_2_0)) {
            extendedPlugins = in.readList(StreamInput::readString);
        } else {
            extendedPlugins = Collections.emptyList();
        }
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
        if (out.getVersion().onOrAfter(Version.V_6_2_0)) {
            out.writeStringList(extendedPlugins);
        }
        if (out.getVersion().onOrAfter(Version.V_5_4_0)) {
            out.writeBoolean(hasNativeController);
        }
        if (out.getVersion().onOrAfter(Version.V_6_0_0_beta2)) {
            out.writeBoolean(requiresKeystore);
        }
    }

    /**
     * Extracts all {@link PluginInfo} from the provided {@code rootPath} expanding meta plugins if needed.
     * @param rootPath the path where the plugins are installed
     * @return A list of all plugin paths installed in the {@code rootPath}
     * @throws IOException if an I/O exception occurred reading the plugin descriptors
     */
    public static List<Path> extractAllPlugins(final Path rootPath) throws IOException {
        final List<Path> plugins = new LinkedList<>();  // order is already lost, but some filesystems have it
        final Set<String> seen = new HashSet<>();
        if (Files.exists(rootPath)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(rootPath)) {
                for (Path plugin : stream) {
                    if (FileSystemUtils.isDesktopServicesStore(plugin) ||
                            plugin.getFileName().toString().startsWith(".removing-")) {
                        continue;
                    }
                    if (seen.add(plugin.getFileName().toString()) == false) {
                        throw new IllegalStateException("duplicate plugin: " + plugin);
                    }
                    if (MetaPluginInfo.isMetaPlugin(plugin)) {
                        try (DirectoryStream<Path> subStream = Files.newDirectoryStream(plugin)) {
                            for (Path subPlugin : subStream) {
                                if (MetaPluginInfo.isPropertiesFile(subPlugin) ||
                                        FileSystemUtils.isDesktopServicesStore(subPlugin)) {
                                    continue;
                                }
                                if (seen.add(subPlugin.getFileName().toString()) == false) {
                                    throw new IllegalStateException("duplicate plugin: " + subPlugin);
                                }
                                plugins.add(subPlugin);
                            }
                        }
                    } else {
                        plugins.add(plugin);
                    }
                }
            }
        }
        return plugins;
    }

    /**
     * Reads and validates the plugin descriptor file.
     *
     * @param path the path to the root directory for the plugin
     * @return the plugin info
     * @throws IOException if an I/O exception occurred reading the plugin descriptor
     */
    public static PluginInfo readFromProperties(final Path path) throws IOException {
        return readFromProperties(path, true);
    }

    /**
     * Reads and validates the plugin descriptor file. If {@code enforceVersion} is false then version enforcement for the plugin descriptor
     * is skipped.
     *
     * @param path           the path to the root directory for the plugin
     * @param enforceVersion whether or not to enforce the version when reading plugin descriptors
     * @return the plugin info
     * @throws IOException if an I/O exception occurred reading the plugin descriptor
     */
    static PluginInfo readFromProperties(final Path path, final boolean enforceVersion) throws IOException {
        final Path descriptor = path.resolve(ES_PLUGIN_PROPERTIES);

        final Map<String, String> propsMap;
        {
            final Properties props = new Properties();
            try (InputStream stream = Files.newInputStream(descriptor)) {
                props.load(stream);
            }
            propsMap = props.stringPropertyNames().stream().collect(Collectors.toMap(Function.identity(), props::getProperty));
        }

        final String name = propsMap.remove("name");
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException(
                    "property [name] is missing in [" + descriptor + "]");
        }
        final String description = propsMap.remove("description");
        if (description == null) {
            throw new IllegalArgumentException(
                    "property [description] is missing for plugin [" + name + "]");
        }
        final String version = propsMap.remove("version");
        if (version == null) {
            throw new IllegalArgumentException(
                    "property [version] is missing for plugin [" + name + "]");
        }

        final String esVersionString = propsMap.remove("elasticsearch.version");
        if (esVersionString == null) {
            throw new IllegalArgumentException(
                    "property [elasticsearch.version] is missing for plugin [" + name + "]");
        }
        final Version esVersion = Version.fromString(esVersionString);
        if (enforceVersion && esVersion.equals(Version.CURRENT) == false) {
            final String message = String.format(
                    Locale.ROOT,
                    "plugin [%s] is incompatible with version [%s]; was designed for version [%s]",
                    name,
                    Version.CURRENT.toString(),
                    esVersionString);
            throw new IllegalArgumentException(message);
        }
        final String javaVersionString = propsMap.remove("java.version");
        if (javaVersionString == null) {
            throw new IllegalArgumentException(
                    "property [java.version] is missing for plugin [" + name + "]");
        }
        JarHell.checkVersionFormat(javaVersionString);
        JarHell.checkJavaVersion(name, javaVersionString);
        final String classname = propsMap.remove("classname");
        if (classname == null) {
            throw new IllegalArgumentException(
                    "property [classname] is missing for plugin [" + name + "]");
        }

        final String extendedString = propsMap.remove("extended.plugins");
        final List<String> extendedPlugins;
        if (extendedString == null) {
            extendedPlugins = Collections.emptyList();
        } else {
            extendedPlugins = Arrays.asList(Strings.delimitedListToStringArray(extendedString, ","));
        }

        final String hasNativeControllerValue = propsMap.remove("has.native.controller");
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

        String requiresKeystoreValue = propsMap.remove("requires.keystore");
        if (requiresKeystoreValue == null) {
            requiresKeystoreValue = "false";
        }
        final boolean requiresKeystore;
        try {
            requiresKeystore = Booleans.parseBoolean(requiresKeystoreValue);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("property [requires.keystore] must be [true] or [false]," +
                    " but was [" + requiresKeystoreValue + "]", e);
        }

        if (propsMap.isEmpty() == false) {
            throw new IllegalArgumentException("Unknown properties in plugin descriptor: " + propsMap.keySet());
        }

        return new PluginInfo(name, description, version, classname, extendedPlugins, hasNativeController, requiresKeystore);
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
     * Other plugins this plugin extends through SPI.
     *
     * @return the names of the plugins extended
     */
    public List<String> getExtendedPlugins() {
        return extendedPlugins;
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
            builder.field("extended_plugins", extendedPlugins);
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
        return toString("");
    }

    public String toString(String prefix) {
        final StringBuilder information = new StringBuilder()
            .append(prefix).append("- Plugin information:\n")
            .append(prefix).append("Name: ").append(name).append("\n")
            .append(prefix).append("Description: ").append(description).append("\n")
            .append(prefix).append("Version: ").append(version).append("\n")
            .append(prefix).append("Native Controller: ").append(hasNativeController).append("\n")
            .append(prefix).append("Requires Keystore: ").append(requiresKeystore).append("\n")
            .append(prefix).append("Extended Plugins: ").append(extendedPlugins).append("\n")
            .append(prefix).append(" * Classname: ").append(classname);
        return information.toString();
    }
}
