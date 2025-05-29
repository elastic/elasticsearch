/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
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
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An in-memory representation of the plugin descriptor.
 */
public class PluginDescriptor implements Writeable, ToXContentObject {

    public static final String INTERNAL_DESCRIPTOR_FILENAME = "plugin-descriptor.properties";
    public static final String STABLE_DESCRIPTOR_FILENAME = "stable-plugin-descriptor.properties";
    public static final String NAMED_COMPONENTS_FILENAME = "named_components.json";

    private static final TransportVersion LICENSED_PLUGINS_SUPPORT = TransportVersions.V_7_11_0;
    private static final TransportVersion MODULE_NAME_SUPPORT = TransportVersions.V_8_3_0;
    private static final TransportVersion BOOTSTRAP_SUPPORT_REMOVED = TransportVersions.V_8_4_0;

    private final String name;
    private final String description;
    private final String version;
    private final String elasticsearchVersion;
    private final String javaVersion;
    private final String classname;
    private final String moduleName;
    private final List<String> extendedPlugins;
    private final boolean hasNativeController;
    private final boolean isLicensed;
    private final boolean isModular;
    private final boolean isStable;

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
     * @param isModular            whether this plugin should be loaded in a module layer
     * @param isStable             whether this plugin is implemented using the stable plugin API
     */
    public PluginDescriptor(
        String name,
        String description,
        String version,
        String elasticsearchVersion,
        String javaVersion,
        String classname,
        String moduleName,
        List<String> extendedPlugins,
        boolean hasNativeController,
        boolean isLicensed,
        boolean isModular,
        boolean isStable
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
        this.isModular = isModular;
        this.isStable = isStable;

        ensureCorrectArgumentsForPluginType();
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
        if (in.getTransportVersion().before(TransportVersions.V_8_12_0)) {
            elasticsearchVersion = Version.readVersion(in).toString();
        } else {
            elasticsearchVersion = in.readString();
        }
        javaVersion = in.readString();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            this.classname = in.readOptionalString();
        } else {
            this.classname = in.readString();
        }
        if (in.getTransportVersion().onOrAfter(MODULE_NAME_SUPPORT)) {
            this.moduleName = in.readOptionalString();
        } else {
            this.moduleName = null;
        }
        extendedPlugins = in.readStringCollectionAsList();
        hasNativeController = in.readBoolean();

        if (in.getTransportVersion().onOrAfter(LICENSED_PLUGINS_SUPPORT)) {
            if (in.getTransportVersion().before(BOOTSTRAP_SUPPORT_REMOVED)) {
                in.readString(); // plugin type
                in.readOptionalString(); // java opts
            }
            isLicensed = in.readBoolean();
        } else {
            isLicensed = false;
        }

        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
            isModular = in.readBoolean();
            isStable = in.readBoolean();
        } else {
            isModular = moduleName != null;
            isStable = false;
        }

        ensureCorrectArgumentsForPluginType();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(description);
        out.writeString(version);
        if (out.getTransportVersion().before(TransportVersions.V_8_12_0)) {
            Version.writeVersion(Version.fromString(elasticsearchVersion), out);
        } else {
            out.writeString(elasticsearchVersion);
        }
        out.writeString(javaVersion);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeOptionalString(classname);
        } else {
            out.writeString(classname);
        }
        if (out.getTransportVersion().onOrAfter(MODULE_NAME_SUPPORT)) {
            out.writeOptionalString(moduleName);
        }
        out.writeStringCollection(extendedPlugins);
        out.writeBoolean(hasNativeController);

        if (out.getTransportVersion().onOrAfter(LICENSED_PLUGINS_SUPPORT)) {
            if (out.getTransportVersion().before(BOOTSTRAP_SUPPORT_REMOVED)) {
                out.writeString("ISOLATED");
                out.writeOptionalString(null);
            }
            out.writeBoolean(isLicensed);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
            out.writeBoolean(isModular);
            out.writeBoolean(isStable);
        }
    }

    private void ensureCorrectArgumentsForPluginType() {
        if (classname == null && isStable == false) {
            throw new IllegalArgumentException("Classname must be provided for classic plugins");
        }
        if (classname != null && isStable) {
            throw new IllegalArgumentException("Classname is not needed for stable plugins");
        }
        if (moduleName != null && isStable) {
            throw new IllegalArgumentException("ModuleName is not needed for stable plugins");
        }
    }

    /**
     * Reads the descriptor file for a plugin.
     *
     * @param pluginDir the path to the root directory for the plugin
     * @return the plugin info
     * @throws IOException if an I/O exception occurred reading the plugin descriptor
     */
    public static PluginDescriptor readFromProperties(final Path pluginDir) throws IOException {
        Path internalDescriptorFile = pluginDir.resolve(INTERNAL_DESCRIPTOR_FILENAME);
        Path stableDescriptorFile = pluginDir.resolve(STABLE_DESCRIPTOR_FILENAME);

        final BiFunction<Map<String, String>, String, PluginDescriptor> reader;
        final Path descriptorFile;

        boolean internalDescriptorExists = Files.exists(internalDescriptorFile);
        boolean stableDescriptorExists = Files.exists(stableDescriptorFile);
        String name = pluginDir.getFileName().toString(); // temporary until descriptor is loaded

        if (internalDescriptorExists && stableDescriptorExists) {
            throw new IllegalStateException(
                "Plugin [" + name + "] has both stable and internal descriptor properties. Only one may exist."
            );
        } else if (internalDescriptorExists == false && stableDescriptorExists == false) {
            throw new IllegalStateException("Plugin [" + name + "] is missing a descriptor properties file.");
        } else if (internalDescriptorExists) {
            descriptorFile = internalDescriptorFile;
            reader = PluginDescriptor::readerInternalDescriptor;
        } else {
            descriptorFile = stableDescriptorFile;
            reader = PluginDescriptor::readerStableDescriptor;
        }

        final Map<String, String> propsMap;
        {
            final Properties props = new Properties();
            try (InputStream stream = Files.newInputStream(descriptorFile)) {
                props.load(stream);
            }
            propsMap = props.stringPropertyNames().stream().collect(Collectors.toMap(Function.identity(), props::getProperty));
        }

        PluginDescriptor descriptor = reader.apply(propsMap, descriptorFile.getFileName().toString());
        name = descriptor.getName();

        if (propsMap.isEmpty() == false) {
            throw new IllegalArgumentException("Unknown properties for plugin [" + name + "] in plugin descriptor: " + propsMap.keySet());
        }

        return descriptor;
    }

    /**
     * Reads the internal descriptor for a classic plugin.
     *
     * @param stream the InputStream from which to read the plugin data
     * @return the plugin info
     * @throws IOException if an I/O exception occurred reading the plugin descriptor
     */
    public static PluginDescriptor readInternalDescriptorFromStream(InputStream stream) throws IOException {
        final Map<String, String> propsMap;
        {
            final Properties props = new Properties();
            props.load(stream);
            propsMap = props.stringPropertyNames().stream().collect(Collectors.toMap(Function.identity(), props::getProperty));
        }

        PluginDescriptor descriptor = readerInternalDescriptor(propsMap, INTERNAL_DESCRIPTOR_FILENAME);
        String name = descriptor.getName();

        if (propsMap.isEmpty() == false) {
            throw new IllegalArgumentException("Unknown properties for plugin [" + name + "] in plugin descriptor: " + propsMap.keySet());
        }

        return descriptor;
    }

    private static PluginDescriptor readerInternalDescriptor(Map<String, String> propsMap, String filename) {
        String name = readNonEmptyString(propsMap, filename, "name");
        String desc = readString(propsMap, name, "description");
        String ver = readString(propsMap, name, "version");
        String esVer = readElasticsearchVersion(propsMap, name);
        String javaVer = readJavaVersion(propsMap, name);

        String extendedString = propsMap.remove("extended.plugins");
        List<String> extended = List.of();
        if (extendedString != null) {
            extended = List.of(Strings.delimitedListToStringArray(extendedString, ","));
        }

        boolean nativeCont = readBoolean(propsMap, name, "has.native.controller");
        String classname = readNonEmptyString(propsMap, name, "classname");
        String module = propsMap.remove("modulename");
        if (module != null && module.isBlank()) {
            module = null;
        }

        boolean isLicensed = readBoolean(propsMap, name, "licensed");
        boolean modular = module != null;

        return new PluginDescriptor(name, desc, ver, esVer, javaVer, classname, module, extended, nativeCont, isLicensed, modular, false);
    }

    private static PluginDescriptor readerStableDescriptor(Map<String, String> propsMap, String filename) {
        String name = readNonEmptyString(propsMap, filename, "name");
        String desc = readString(propsMap, name, "description");
        String ver = readString(propsMap, name, "version");
        String esVer = readElasticsearchVersion(propsMap, name);
        String javaVer = readJavaVersion(propsMap, name);
        boolean isModular = readBoolean(propsMap, name, "modular");

        return new PluginDescriptor(name, desc, ver, esVer, javaVer, null, null, List.of(), false, false, isModular, true);
    }

    private static String readNonEmptyString(Map<String, String> propsMap, String pluginId, String name) {
        String value = propsMap.remove(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("property [" + name + "] is missing for plugin [" + pluginId + "]");
        }
        return value;
    }

    private static String readString(Map<String, String> propsMap, String pluginId, String name) {
        String value = propsMap.remove(name);
        if (value == null) {
            throw new IllegalArgumentException("property [" + name + "] is missing for plugin [" + pluginId + "]");
        }
        return value;
    }

    private static String readElasticsearchVersion(Map<String, String> propsMap, String pluginId) {
        return readNonEmptyString(propsMap, pluginId, "elasticsearch.version");
    }

    private static String readJavaVersion(Map<String, String> propsMap, String pluginId) {
        String javaVersion = readNonEmptyString(propsMap, pluginId, "java.version");
        JarHell.checkJavaVersion("plugin " + pluginId, javaVersion);
        return javaVersion;
    }

    private static boolean readBoolean(Map<String, String> propsMap, String pluginId, String name) {
        String rawValue = propsMap.remove(name);
        try {
            return Booleans.parseBoolean(rawValue, false);
        } catch (IllegalArgumentException e) {
            final String message = String.format(
                Locale.ROOT,
                "property [%s] must be [true], [false], or unspecified but was [%s] for plugin [%s]",
                name,
                rawValue,
                pluginId
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
        if (isStable) {
            throw new IllegalStateException("Stable plugins do not have an explicit entry point");
        }
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
    public String getElasticsearchVersion() {
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

    /**
     * Whether this plugin should be loaded in a module layer.
     */
    public boolean isModular() {
        return isModular;
    }

    /**
     * Whether this plugin uses only stable APIs.
     */
    public boolean isStable() {
        return isStable;
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
