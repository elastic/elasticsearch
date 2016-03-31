/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import org.elasticsearch.Version;
import org.elasticsearch.bootstrap.JarHell;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class XPackExtensionInfo {
    public static final String XPACK_EXTENSION_PROPERTIES = "x-pack-extension-descriptor.properties";

    private String name;
    private String description;
    private String version;
    private String classname;

    public XPackExtensionInfo() {
    }

    /**
     * Information about extensions
     *
     * @param name        Its name
     * @param description Its description
     * @param version     Version number
     */
    XPackExtensionInfo(String name, String description, String version, String classname) {
        this.name = name;
        this.description = description;
        this.version = version;
        this.classname = classname;
    }

    /** reads (and validates) extension metadata descriptor file */
    public static XPackExtensionInfo readFromProperties(Path dir) throws IOException {
        Path descriptor = dir.resolve(XPACK_EXTENSION_PROPERTIES);
        Properties props = new Properties();
        try (InputStream stream = Files.newInputStream(descriptor)) {
            props.load(stream);
        }
        String name = props.getProperty("name");
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Property [name] is missing in [" + descriptor + "]");
        }
        String description = props.getProperty("description");
        if (description == null) {
            throw new IllegalArgumentException("Property [description] is missing for extension [" + name + "]");
        }
        String version = props.getProperty("version");
        if (version == null) {
            throw new IllegalArgumentException("Property [version] is missing for extension [" + name + "]");
        }

        String xpackVersionString = props.getProperty("xpack.version");
        if (xpackVersionString == null) {
            throw new IllegalArgumentException("Property [xpack.version] is missing for extension [" + name + "]");
        }
        Version xpackVersion = Version.fromString(xpackVersionString);
        if (xpackVersion.equals(Version.CURRENT) == false) {
            throw new IllegalArgumentException("extension [" + name + "] is incompatible with Elasticsearch [" +
                    Version.CURRENT.toString() + "]. Was designed for version [" + xpackVersionString + "]");
        }
        String javaVersionString = props.getProperty("java.version");
        if (javaVersionString == null) {
            throw new IllegalArgumentException("Property [java.version] is missing for extension [" + name + "]");
        }
        JarHell.checkVersionFormat(javaVersionString);
        JarHell.checkJavaVersion(name, javaVersionString);
        String classname = props.getProperty("classname");
        if (classname == null) {
            throw new IllegalArgumentException("Property [classname] is missing for extension [" + name + "]");
        }

        return new XPackExtensionInfo(name, description, version, classname);
    }

    /**
     * @return Extension's name
     */
    public String getName() {
        return name;
    }

    /**
     * @return Extension's description if any
     */
    public String getDescription() {
        return description;
    }

    /**
     * @return extension's classname
     */
    public String getClassname() {
        return classname;
    }

    /**
     * @return Version number for the extension
     */
    public String getVersion() {
        return version;
    }

    @Override
    public String toString() {
        final StringBuilder information = new StringBuilder()
                .append("- XPack Extension information:\n")
                .append("Name: ").append(name).append("\n")
                .append("Description: ").append(description).append("\n")
                .append("Version: ").append(version).append("\n")
                .append(" * Classname: ").append(classname);

        return information.toString();
    }
}
