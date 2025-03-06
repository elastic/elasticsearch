/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.useragent;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class UserAgentProcessorFactoryTests extends ESTestCase {

    private static Map<String, UserAgentParser> userAgentParsers;

    private static String regexWithoutDevicesFilename = "regexes_without_devices.yml";
    private static Path userAgentConfigDir;

    @BeforeClass
    public static void createUserAgentParsers() throws IOException {
        Path configDir = createTempDir();
        userAgentConfigDir = configDir.resolve("ingest-user-agent");
        Files.createDirectories(userAgentConfigDir);

        // Copy file, leaving out the device parsers at the end
        try (
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(UserAgentProcessor.class.getResourceAsStream("/regexes.yml"), StandardCharsets.UTF_8)
            );
            BufferedWriter writer = Files.newBufferedWriter(userAgentConfigDir.resolve(regexWithoutDevicesFilename));
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("device_parsers:")) {
                    break;
                }

                writer.write(line);
                writer.newLine();
            }
        }

        userAgentParsers = IngestUserAgentPlugin.createUserAgentParsers(userAgentConfigDir, new UserAgentCache(1000));
    }

    public void testBuildDefaults() throws Exception {
        UserAgentProcessor.Factory factory = new UserAgentProcessor.Factory(userAgentParsers);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");

        String processorTag = randomAlphaOfLength(10);

        UserAgentProcessor processor = factory.create(null, processorTag, null, config, null);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("user_agent"));
        assertThat(processor.getUaParser().getUaPatterns().size(), greaterThan(0));
        assertThat(processor.getUaParser().getOsPatterns().size(), greaterThan(0));
        assertThat(processor.getUaParser().getDevicePatterns().size(), greaterThan(0));
        assertThat(processor.getProperties(), equalTo(EnumSet.allOf(UserAgentProcessor.Property.class)));
        assertFalse(processor.isExtractDeviceType());
        assertFalse(processor.isIgnoreMissing());
    }

    public void testBuildWithIgnoreMissing() throws Exception {
        UserAgentProcessor.Factory factory = new UserAgentProcessor.Factory(userAgentParsers);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("ignore_missing", true);

        String processorTag = randomAlphaOfLength(10);

        UserAgentProcessor processor = factory.create(null, processorTag, null, config, null);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("user_agent"));
        assertThat(processor.getUaParser().getUaPatterns().size(), greaterThan(0));
        assertThat(processor.getUaParser().getOsPatterns().size(), greaterThan(0));
        assertThat(processor.getUaParser().getDevicePatterns().size(), greaterThan(0));
        assertThat(processor.getProperties(), equalTo(EnumSet.allOf(UserAgentProcessor.Property.class)));
        assertTrue(processor.isIgnoreMissing());
    }

    public void testBuildTargetField() throws Exception {
        UserAgentProcessor.Factory factory = new UserAgentProcessor.Factory(userAgentParsers);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("target_field", "_target_field");

        UserAgentProcessor processor = factory.create(null, null, null, config, null);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getTargetField(), equalTo("_target_field"));
    }

    public void testBuildRegexFile() throws Exception {
        UserAgentProcessor.Factory factory = new UserAgentProcessor.Factory(userAgentParsers);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("regex_file", regexWithoutDevicesFilename);

        UserAgentProcessor processor = factory.create(null, null, null, config, null);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getUaParser().getUaPatterns().size(), greaterThan(0));
        assertThat(processor.getUaParser().getOsPatterns().size(), greaterThan(0));
        assertThat(processor.getUaParser().getDevicePatterns().size(), equalTo(0));
    }

    public void testBuildExtractDeviceType() throws Exception {
        UserAgentProcessor.Factory factory = new UserAgentProcessor.Factory(userAgentParsers);
        boolean extractDeviceType = randomBoolean();

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("extract_device_type", extractDeviceType);

        UserAgentProcessor processor = factory.create(null, null, null, config, null);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.isExtractDeviceType(), equalTo(extractDeviceType));
    }

    public void testBuildNonExistingRegexFile() throws Exception {
        UserAgentProcessor.Factory factory = new UserAgentProcessor.Factory(userAgentParsers);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("regex_file", "does-not-exist.yml");

        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, null, null, config, null)
        );
        assertThat(e.getMessage(), equalTo("[regex_file] regex file [does-not-exist.yml] doesn't exist (has to exist at node startup)"));
    }

    public void testBuildFields() throws Exception {
        UserAgentProcessor.Factory factory = new UserAgentProcessor.Factory(userAgentParsers);

        Set<UserAgentProcessor.Property> properties = EnumSet.noneOf(UserAgentProcessor.Property.class);
        List<String> fieldNames = new ArrayList<>();
        int numFields = scaledRandomIntBetween(1, UserAgentProcessor.Property.values().length);
        for (int i = 0; i < numFields; i++) {
            UserAgentProcessor.Property property = UserAgentProcessor.Property.values()[i];
            properties.add(property);
            fieldNames.add(property.name().toLowerCase(Locale.ROOT));
        }

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", fieldNames);

        UserAgentProcessor processor = factory.create(null, null, null, config, null);
        assertThat(processor.getField(), equalTo("_field"));
        assertThat(processor.getProperties(), equalTo(properties));
    }

    public void testInvalidProperty() throws Exception {
        UserAgentProcessor.Factory factory = new UserAgentProcessor.Factory(userAgentParsers);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", Collections.singletonList("invalid"));

        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, null, null, config, null)
        );
        assertThat(
            e.getMessage(),
            equalTo("[properties] illegal property value [invalid]. valid values are [NAME, OS, DEVICE, " + "ORIGINAL, VERSION]")
        );
    }

    public void testInvalidPropertiesType() throws Exception {
        UserAgentProcessor.Factory factory = new UserAgentProcessor.Factory(userAgentParsers);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("properties", "invalid");

        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, null, null, config, null)
        );
        assertThat(e.getMessage(), equalTo("[properties] property isn't a list, but of type [java.lang.String]"));
    }
}
