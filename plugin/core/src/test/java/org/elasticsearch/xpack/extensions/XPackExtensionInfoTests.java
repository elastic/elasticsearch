/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;

public class XPackExtensionInfoTests extends ESTestCase {

    public void testReadFromProperties() throws Exception {
        Path extensionDir = createTempDir().resolve("fake-extension");
        XPackExtensionTestUtil.writeProperties(extensionDir,
                "description", "fake desc",
                "name", "my_extension",
                "version", "1.0",
                "xpack.version", Version.CURRENT.toString(),
                "java.version", System.getProperty("java.specification.version"),
                "classname", "FakeExtension");
        XPackExtensionInfo info = XPackExtensionInfo.readFromProperties(extensionDir);
        assertEquals("my_extension", info.getName());
        assertEquals("fake desc", info.getDescription());
        assertEquals("1.0", info.getVersion());
        assertEquals("FakeExtension", info.getClassname());
    }

    public void testReadFromPropertiesNameMissing() throws Exception {
        Path extensionDir = createTempDir().resolve("fake-extension");
        XPackExtensionTestUtil.writeProperties(extensionDir);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            XPackExtensionInfo.readFromProperties(extensionDir);
        });
        assertTrue(e.getMessage().contains("Property [name] is missing in"));
        XPackExtensionTestUtil.writeProperties(extensionDir, "name", "");
        IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> {
            XPackExtensionInfo.readFromProperties(extensionDir);
        });
        assertTrue(e1.getMessage().contains("Property [name] is missing in"));
    }

    public void testReadFromPropertiesDescriptionMissing() throws Exception {
        Path extensionDir = createTempDir().resolve("fake-extension");
        XPackExtensionTestUtil.writeProperties(extensionDir, "name", "fake-extension");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            XPackExtensionInfo.readFromProperties(extensionDir);
        });
        assertTrue(e.getMessage().contains("[description] is missing"));
    }

    public void testReadFromPropertiesVersionMissing() throws Exception {
        Path extensionDir = createTempDir().resolve("fake-extension");
        XPackExtensionTestUtil.writeProperties(extensionDir, "description", "fake desc", "name", "fake-extension");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            XPackExtensionInfo.readFromProperties(extensionDir);
        });
        assertTrue(e.getMessage().contains("[version] is missing"));
    }

    public void testReadFromPropertiesElasticsearchVersionMissing() throws Exception {
        Path extensionDir = createTempDir().resolve("fake-extension");
        XPackExtensionTestUtil.writeProperties(extensionDir,
                "description", "fake desc",
                "name", "my_extension",
                "version", "1.0");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            XPackExtensionInfo.readFromProperties(extensionDir);
        });
        assertTrue(e.getMessage().contains("[xpack.version] is missing"));
    }

    public void testReadFromPropertiesJavaVersionMissing() throws Exception {
        Path extensionDir = createTempDir().resolve("fake-extension");
        XPackExtensionTestUtil.writeProperties(extensionDir,
                "description", "fake desc",
                "name", "my_extension",
                "xpack.version", Version.CURRENT.toString(),
                "version", "1.0");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            XPackExtensionInfo.readFromProperties(extensionDir);
        });
        assertTrue(e.getMessage().contains("[java.version] is missing"));
    }

    public void testReadFromPropertiesJavaVersionIncompatible() throws Exception {
        String extensionName = "fake-extension";
        Path extensionDir = createTempDir().resolve(extensionName);
        XPackExtensionTestUtil.writeProperties(extensionDir,
                "description", "fake desc",
                "name", extensionName,
                "xpack.version", Version.CURRENT.toString(),
                "java.version", "1000000.0",
                "classname", "FakeExtension",
                "version", "1.0");
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
            XPackExtensionInfo.readFromProperties(extensionDir);
        });
        assertTrue(e.getMessage(), e.getMessage().contains(extensionName + " requires Java"));
    }

    public void testReadFromPropertiesBadJavaVersionFormat() throws Exception {
        String extensionName = "fake-extension";
        Path extensionDir = createTempDir().resolve(extensionName);
        XPackExtensionTestUtil.writeProperties(extensionDir,
                "description", "fake desc",
                "name", extensionName,
                "xpack.version", Version.CURRENT.toString(),
                "java.version", "1.7.0_80",
                "classname", "FakeExtension",
                "version", "1.0");
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> {
            XPackExtensionInfo.readFromProperties(extensionDir);
        });
        assertTrue(e.getMessage(),
                    e.getMessage().equals("version string must be a sequence of nonnegative decimal " +
                            "integers separated by \".\"'s and may have leading zeros but was 1.7.0_80"));
    }

    public void testReadFromPropertiesBogusElasticsearchVersion() throws Exception {
        Path extensionDir = createTempDir().resolve("fake-extension");
        XPackExtensionTestUtil.writeProperties(extensionDir,
                "description", "fake desc",
                "version", "1.0",
                "name", "my_extension",
                "xpack.version", "bogus");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            XPackExtensionInfo.readFromProperties(extensionDir);
        });
        assertTrue(e.getMessage().contains("version needs to contain major, minor, and revision"));
    }

    public void testReadFromPropertiesOldElasticsearchVersion() throws Exception {
        Path extensionDir = createTempDir().resolve("fake-extension");
        XPackExtensionTestUtil.writeProperties(extensionDir,
                "description", "fake desc",
                "name", "my_extension",
                "version", "1.0",
                "xpack.version", Version.V_5_0_0.toString());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            XPackExtensionInfo.readFromProperties(extensionDir);
        });
        assertTrue(e.getMessage().contains("Was designed for version [5.0.0]"));
    }

    public void testReadFromPropertiesJvmMissingClassname() throws Exception {
        Path extensionDir = createTempDir().resolve("fake-extension");
        XPackExtensionTestUtil.writeProperties(extensionDir,
                "description", "fake desc",
                "name", "my_extension",
                "version", "1.0",
                "xpack.version", Version.CURRENT.toString(),
                "java.version", System.getProperty("java.specification.version"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            XPackExtensionInfo.readFromProperties(extensionDir);
        });
        assertTrue(e.getMessage().contains("Property [classname] is missing"));
    }
}
