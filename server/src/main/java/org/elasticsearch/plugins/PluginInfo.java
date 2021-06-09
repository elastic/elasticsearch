/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.Version;
import org.elasticsearch.jdk.JarHell;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An in-memory representation of the plugin descriptor.
 */
public class PluginInfo implements Writeable, ToXContentObject {

    public static final String ES_PLUGIN_PROPERTIES = "plugin-descriptor.properties";
    public static final String ES_PLUGIN_POLICY = "plugin-security.policy";

    private static final Version LICENSED_PLUGINS_SUPPORT = Version.V_7_11_0;

    private final String name;
    private final String description;
    private final String version;
    private final Version elasticsearchVersion;
    private final String javaVersion;
    private final String classname;
    private final List<String> extendedPlugins;
    private final boolean hasNativeController;
    private final PluginType type;
    private final String javaOpts;
    private final boolean isLicensed;

    /**
     * Construct plugin info.
     *
     * @param name                  the name of the plugin
     * @param description           a description of the plugin
     * @param version               an opaque version identifier for the plugin
     * @param elasticsearchVersion  the version of Elasticsearch the plugin was built for
     * @param javaVersion           the version of Java the plugin was built with
     * @param classname             the entry point to the plugin
     * @param extendedPlugins       other plugins this plugin extends through SPI
     * @param hasNativeController   whether or not the plugin has a native controller
     * @param type                  the type of the plugin. Expects "bootstrap" or "isolated".
     * @param javaOpts              any additional JVM CLI parameters added by this plugin
     * @param isLicensed            whether is this a licensed plugin
     */
    public PluginInfo(String name, String description, String version, Version elasticsearchVersion, String javaVersion,
                      String classname, List<String> extendedPlugins, boolean hasNativeController,
                      PluginType type, String javaOpts, boolean isLicensed) {
        this.name = name;
        this.description = description;
        this.version = version;
        this.elasticsearchVersion = elasticsearchVersion;
        this.javaVersion = javaVersion;
        this.classname = classname;
        this.extendedPlugins = Collections.unmodifiableList(extendedPlugins);
        this.hasNativeController = hasNativeController;
        this.type = type;
        this.javaOpts = javaOpts;
        this.isLicensed = isLicensed;
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
        elasticsearchVersion = Version.readVersion(in);
        javaVersion = in.readString();
        this.classname = in.readString();
        extendedPlugins = in.readStringList();
        hasNativeController = in.readBoolean();

        if (in.getVersion().onOrAfter(LICENSED_PLUGINS_SUPPORT)) {
            type = PluginType.valueOf(in.readString());
            javaOpts = in.readOptionalString();
            isLicensed = in.readBoolean();
        } else {
            type = PluginType.ISOLATED;
            javaOpts = null;
            isLicensed = false;
        }
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(description);
        out.writeString(version);
        Version.writeVersion(elasticsearchVersion, out);
        out.writeString(javaVersion);
        out.writeString(classname);
        out.writeStringCollection(extendedPlugins);
        out.writeBoolean(hasNativeController);

        if (out.getVersion().onOrAfter(LICENSED_PLUGINS_SUPPORT)) {
            out.writeString(type.name());
            out.writeOptionalString(javaOpts);
            out.writeBoolean(isLicensed);
        }
    }

    /**
     * Reads the plugin descriptor file.
     *
     * @param path           the path to the root directory for the plugin
     * @return the plugin info
     * @throws IOException if an I/O exception occurred reading the plugin descriptor
     */
    public static PluginInfo readFromProperties(final Path path) throws IOException {
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
        final String javaVersionString = propsMap.remove("java.version");
        if (javaVersionString == null) {
            throw new IllegalArgumentException(
                    "property [java.version] is missing for plugin [" + name + "]");
        }
        JarHell.checkVersionFormat(javaVersionString);

        final String extendedString = propsMap.remove("extended.plugins");
        final List<String> extendedPlugins;
        if (extendedString == null) {
            extendedPlugins = Collections.emptyList();
        } else {
            extendedPlugins = Arrays.asList(Strings.delimitedListToStringArray(extendedString, ","));
        }

        final boolean hasNativeController = parseBooleanValue(name, "has.native.controller", propsMap.remove("has.native.controller"));

        final PluginType type = getPluginType(name, propsMap.remove("type"));

        final String classname = getClassname(name, type, propsMap.remove("classname"));

        final String javaOpts = propsMap.remove("java.opts");

        if (type != PluginType.BOOTSTRAP && Strings.isNullOrEmpty(javaOpts) == false) {
            throw new IllegalArgumentException(
                "[java.opts] can only have a value when [type] is set to [bootstrap] for plugin [" + name + "]"
            );
        }

        boolean isLicensed = parseBooleanValue(name, "licensed", propsMap.remove("licensed"));

        if (propsMap.isEmpty() == false) {
            throw new IllegalArgumentException("Unknown properties for plugin [" + name + "] in plugin descriptor: " + propsMap.keySet());
        }

        return new PluginInfo(name, description, version, esVersion, javaVersionString,
                              classname, extendedPlugins, hasNativeController, type, javaOpts, isLicensed);
    }

    private static PluginType getPluginType(String name, String rawType) {
        if (Strings.isNullOrEmpty(rawType)) {
            return PluginType.ISOLATED;
        }

        try {
            return PluginType.valueOf(rawType.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "[type] must be unspecified or one of [isolated, bootstrap] but found [" + rawType + "] for plugin [" + name + "]"
            );
        }
    }

    private static String getClassname(String name, PluginType type, String classname) {
        if (type == PluginType.BOOTSTRAP) {
            if (Strings.isNullOrEmpty(classname) == false) {
                throw new IllegalArgumentException(
                    "property [classname] can only have a value when [type] is set to [bootstrap] for plugin [" + name + "]"
                );
            }
            return "";
        }

        if (classname == null) {
            throw new IllegalArgumentException("property [classname] is missing for plugin [" + name + "]");
        }

        return classname;
    }

    private static boolean parseBooleanValue(String pluginName, String name, String rawValue) {
        try {
            return Booleans.parseBoolean(rawValue, false);
        } catch (IllegalArgumentException e) {
            final String message = String.format(
                Locale.ROOT,
                "property [%s] must be [true], [false], or unspecified but was [%s] for plugin [%s]",
                name,
                rawValue,
                pluginName
            );
            throw new IllegalArgumentException(message);
        }
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
     * The version of the plugin
     *
     * @return the version
     */
    public String getVersion() {
        return version;
    }

    /**
     * The version of Elasticsearch the plugin was built for.
     *
     * @return an Elasticsearch version
     */
    public Version getElasticsearchVersion() {
        return elasticsearchVersion;
    }

    /**
     * The version of Java the plugin was built with.
     *
     * @return a java version string
     */
    public String getJavaVersion() {
        return javaVersion;
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
     * Returns the type of this plugin. Can be "isolated" for regular sandboxed plugins, or "bootstrap"
     * for plugins that affect how Elasticsearch's JVM runs.
     *
     * @return the type of the plugin
     */
    public PluginType getType() {
        return type;
    }

    /**
     * Returns any additional JVM command-line options that this plugin adds. Only applies to
     * plugins whose <code>type</code> is "bootstrap".
     *
     * @return any additional JVM options.
     */
    public String getJavaOpts() {
        return javaOpts;
    }

    /**
     * Whether this plugin is subject to the Elastic License.
     */
    public boolean isLicensed() {
        return isLicensed;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            builder.field("name", name);
            builder.field("version", version);
            builder.field("elasticsearch_version", elasticsearchVersion);
            builder.field("java_version", javaVersion);
            builder.field("description", description);
            builder.field("classname", classname);
            builder.field("extended_plugins", extendedPlugins);
            builder.field("has_native_controller", hasNativeController);
            builder.field("licensed", isLicensed);
            builder.field("type", type);
            if (type == PluginType.BOOTSTRAP) {
                builder.field("java_opts", javaOpts);
            }
        }
        builder.endObject();

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PluginInfo that = (PluginInfo) o;

        if (name.equals(that.name) == false) return false;
        // TODO: since the plugins are unique by their directory name, this should only be a name check, version should not matter?
        return Objects.equals(version, that.version);
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
            .append(prefix).append("Elasticsearch Version: ").append(elasticsearchVersion).append("\n")
            .append(prefix).append("Java Version: ").append(javaVersion).append("\n")
            .append(prefix).append("Native Controller: ").append(hasNativeController).append("\n")
            .append(prefix).append("Licensed: ").append(isLicensed).append("\n")
            .append(prefix).append("Type: ").append(type).append("\n");

        if (type == PluginType.BOOTSTRAP) {
            information.append(prefix).append("Java Opts: ").append(javaOpts).append("\n");
        }

        information
            .append(prefix).append("Extended Plugins: ").append(extendedPlugins).append("\n")
            .append(prefix).append(" * Classname: ").append(classname);
        return information.toString();
    }
}
