/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.jdk.JarHell;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An in-memory representation of the plugin descriptor.
 */
public class PluginDescriptor implements Writeable, ToXContentObject {

    public static final String ES_PLUGIN_PROPERTIES = "plugin-descriptor.properties";
    public static final String ES_PLUGIN_POLICY = "plugin-security.policy";

    private static final Version LICENSED_PLUGINS_SUPPORT = Version.V_7_11_0;
    private static final Version MODULE_NAME_SUPPORT = Version.V_8_3_0;
    private static final Version BOOTSTRAP_SUPPORT_REMOVED = Version.V_8_4_0;

    private final String name;
    private final String description;
    private final String version;
    private final Version elasticsearchVersion;
    private final String javaVersion;
    private final String classname;
    private final String moduleName;
    private final List<String> extendedPlugins;
    private final boolean hasNativeController;
    private final boolean isLicensed;

    /**
     * Construct plugin info.
     *
     * @param name                 the name of the plugin
     * @param description          a description of the plugin
     * @param version              an opaque version identifier for the plugin
     * @param elasticsearchVersion the version of Elasticsearch the plugin was built for
     * @param javaVersion          the version of Java the plugin was built with
     * @param classname            the entry point to the plugin
     * @param moduleName           the module name to load the plugin class from, or null if not in a module
     * @param extendedPlugins      other plugins this plugin extends through SPI
     * @param hasNativeController  whether or not the plugin has a native controller
     * @param isLicensed           whether is this a licensed plugin
     */
    public PluginDescriptor(
        String name,
        String description,
        String version,
        Version elasticsearchVersion,
        String javaVersion,
        String classname,
        String moduleName,
        List<String> extendedPlugins,
        boolean hasNativeController,
        boolean isLicensed
    ) {
        this.name = name;
        this.description = description;
        this.version = version;
        this.elasticsearchVersion = elasticsearchVersion;
        this.javaVersion = javaVersion;
        this.classname = classname;
        this.moduleName = moduleName;
        this.extendedPlugins = Collections.unmodifiableList(extendedPlugins);
        this.hasNativeController = hasNativeController;
        this.isLicensed = isLicensed;
    }

    /**
     * Construct plugin info from a stream.
     *
     * @param in the stream
     * @throws IOException if an I/O exception occurred reading the plugin info from the stream
     */
    public PluginDescriptor(final StreamInput in) throws IOException {
        this.name = in.readString();
        this.description = in.readString();
        this.version = in.readString();
        elasticsearchVersion = Version.readVersion(in);
        javaVersion = in.readString();
        this.classname = in.readString();
        if (in.getVersion().onOrAfter(MODULE_NAME_SUPPORT)) {
            this.moduleName = in.readOptionalString();
        } else {
            this.moduleName = null;
        }
        extendedPlugins = in.readStringList();
        hasNativeController = in.readBoolean();

        if (in.getVersion().onOrAfter(LICENSED_PLUGINS_SUPPORT)) {
            if (in.getVersion().before(BOOTSTRAP_SUPPORT_REMOVED)) {
                in.readString(); // plugin type
                in.readOptionalString(); // java opts
            }
            isLicensed = in.readBoolean();
        } else {
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
        if (out.getVersion().onOrAfter(MODULE_NAME_SUPPORT)) {
            out.writeOptionalString(moduleName);
        }
        out.writeStringCollection(extendedPlugins);
        out.writeBoolean(hasNativeController);

        if (out.getVersion().onOrAfter(LICENSED_PLUGINS_SUPPORT)) {
            if (out.getVersion().before(BOOTSTRAP_SUPPORT_REMOVED)) {
                out.writeString("ISOLATED");
                out.writeOptionalString(null);
            }
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
    public static PluginDescriptor readFromProperties(final Path path) throws IOException {
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
            throw new IllegalArgumentException("property [name] is missing in [" + descriptor + "]");
        }
        final String description = propsMap.remove("description");
        if (description == null) {
            throw new IllegalArgumentException("property [description] is missing for plugin [" + name + "]");
        }
        final String version = propsMap.remove("version");
        if (version == null) {
            throw new IllegalArgumentException("property [version] is missing for plugin [" + name + "]");
        }

        final String esVersionString = propsMap.remove("elasticsearch.version");
        if (Strings.hasText(esVersionString) == false) {
            throw new IllegalArgumentException("property [elasticsearch.version] is missing for plugin [" + name + "]");
        }
        final Version esVersion = Version.fromString(esVersionString);
        final String javaVersionString = propsMap.remove("java.version");
        if (javaVersionString == null) {
            throw new IllegalArgumentException("property [java.version] is missing for plugin [" + name + "]");
        }
        JarHell.checkJavaVersion("plugin " + name, javaVersionString);

        final String extendedString = propsMap.remove("extended.plugins");
        final List<String> extendedPlugins;
        if (extendedString == null) {
            extendedPlugins = Collections.emptyList();
        } else {
            extendedPlugins = Arrays.asList(Strings.delimitedListToStringArray(extendedString, ","));
        }

        final boolean hasNativeController = parseBooleanValue(name, "has.native.controller", propsMap.remove("has.native.controller"));

        final String classname = getClassname(name, propsMap.remove("classname"));
        String modulename = propsMap.remove("modulename");
        if (modulename != null && modulename.isBlank()) {
            modulename = null;
        }

        boolean isLicensed = parseBooleanValue(name, "licensed", propsMap.remove("licensed"));

        if (propsMap.isEmpty() == false) {
            throw new IllegalArgumentException("Unknown properties for plugin [" + name + "] in plugin descriptor: " + propsMap.keySet());
        }

        return new PluginDescriptor(
            name,
            description,
            version,
            esVersion,
            javaVersionString,
            classname,
            modulename,
            extendedPlugins,
            hasNativeController,
            isLicensed
        );
    }

    private static String getClassname(String name, String classname) {
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
     * The module name of the plugin.
     *
     * @return the module name of the plugin
     */
    public Optional<String> getModuleName() {
        return Optional.ofNullable(moduleName);
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
     * Whether this plugin is subject to the Elastic License.
     */
    public boolean isLicensed() {
        return isLicensed;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragment(builder, params);
        builder.endObject();
        return builder;
    }

    public XContentBuilder toXContentFragment(XContentBuilder builder, Params params) throws IOException {
        builder.field("name", name);
        builder.field("version", version);
        builder.field("elasticsearch_version", elasticsearchVersion);
        builder.field("java_version", javaVersion);
        builder.field("description", description);
        builder.field("classname", classname);
        builder.field("extended_plugins", extendedPlugins);
        builder.field("has_native_controller", hasNativeController);
        builder.field("licensed", isLicensed);

        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PluginDescriptor that = (PluginDescriptor) o;

        return Objects.equals(name, that.name);
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
        final List<String> lines = new ArrayList<>();

        appendLine(lines, prefix, "- Plugin information:", "");
        appendLine(lines, prefix, "Name: ", name);
        appendLine(lines, prefix, "Description: ", description);
        appendLine(lines, prefix, "Version: ", version);
        appendLine(lines, prefix, "Elasticsearch Version: ", elasticsearchVersion);
        appendLine(lines, prefix, "Java Version: ", javaVersion);
        appendLine(lines, prefix, "Native Controller: ", hasNativeController);
        appendLine(lines, prefix, "Licensed: ", isLicensed);
        appendLine(lines, prefix, "Extended Plugins: ", extendedPlugins.toString());
        appendLine(lines, prefix, " * Classname: ", classname);

        return String.join(System.lineSeparator(), lines);
    }

    private static void appendLine(List<String> builder, String prefix, String field, Object value) {
        builder.add(prefix + field + value);
    }
}
