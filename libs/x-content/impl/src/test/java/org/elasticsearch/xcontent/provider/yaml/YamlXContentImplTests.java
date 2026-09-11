/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider.yaml;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class YamlXContentImplTests extends ESTestCase {

    /**
     * SnakeYAML defaults {@code LoaderOptions.codePointLimit} to 3 MB. The Jackson formats (JSON/SMILE/CBOR) already relax
     * their equivalent document-size guards to {@link Integer#MAX_VALUE}, so YAML must do the same to stay consistent.
     * A document larger than the old 3 MB ceiling must parse rather than throw
     * "The incoming YAML document exceeds the limit: 3145728 code points".
     */
    public void testParsesYamlLargerThanDefaultCodePointLimit() throws IOException {
        int valueLength = 4 * 1024 * 1024; // comfortably past the old 3 * 1024 * 1024 ceiling
        String value = "a".repeat(valueLength);
        String yaml = "key: " + value + "\n";
        assertThat(yaml.length(), greaterThan(3 * 1024 * 1024));

        XContent yamlXContent = XContentType.YAML.xContent();
        try (XContentParser parser = yamlXContent.createParser(XContentParserConfiguration.EMPTY, yaml)) {
            Map<String, Object> map = parser.map();
            assertThat(map.get("key"), equalTo(value));
        }
    }
}
