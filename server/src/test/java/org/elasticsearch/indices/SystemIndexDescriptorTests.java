/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.elasticsearch.indices.SystemIndexDescriptor.findDynamicMapping;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SystemIndexDescriptorTests extends ESTestCase {

    /**
     * Tests the various validation rules that are applied when creating a new system index descriptor.
     */
    public void testValidation() {
        {
            Exception ex = expectThrows(NullPointerException.class,
                () -> new SystemIndexDescriptor(null, randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must not be null"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class,
                () -> new SystemIndexDescriptor("", randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must at least 2 characters in length"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class,
                () -> new SystemIndexDescriptor(".", randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must at least 2 characters in length"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class,
                () -> new SystemIndexDescriptor(randomAlphaOfLength(10), randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must start with the character [.]"));
        }

        {
            Exception ex = expectThrows(IllegalArgumentException.class,
                () -> new SystemIndexDescriptor(".*", randomAlphaOfLength(5)));
            assertThat(ex.getMessage(), containsString("must not start with the character sequence [.*] to prevent conflicts"));
        }
        {
            Exception ex = expectThrows(
                IllegalArgumentException.class,
                () -> new SystemIndexDescriptor(".*" + randomAlphaOfLength(10), randomAlphaOfLength(5))
            );
            assertThat(ex.getMessage(), containsString("must not start with the character sequence [.*] to prevent conflicts"));
        }
        {
            final String primaryIndex = randomAlphaOfLength(5);
            Exception ex = expectThrows(
                IllegalArgumentException.class,
                () -> SystemIndexDescriptor.builder().setIndexPattern("." + primaryIndex).setPrimaryIndex(primaryIndex).build()
            );
            assertThat(
                ex.getMessage(),
                equalTo("system primary index provided as [" + primaryIndex + "] but must start with the character [.]")
            );
        }
        {
            final String primaryIndex = "." + randomAlphaOfLength(5) + "*";
            Exception ex = expectThrows(
                IllegalArgumentException.class,
                () -> SystemIndexDescriptor.builder().setIndexPattern("." + randomAlphaOfLength(5)).setPrimaryIndex(primaryIndex).build()
            );
            assertThat(
                ex.getMessage(),
                equalTo("system primary index provided as [" + primaryIndex + "] but cannot contain special characters or patterns")
            );
        }
    }

    /**
     * Check that a system index descriptor correctly identifies the presence of a dynamic mapping when once is present.
     */
    public void testFindDynamicMappingsWithDynamicMapping() {
        String json = "{"
            + "  \"foo\": {"
            + "    \"bar\": {"
            + "      \"dynamic\": false"
            + "    },"
            + "    \"baz\": {"
            + "      \"dynamic\": true"
            + "    }"
            + "  }"
            + "}";

        final Map<String, Object> mappings = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);

        assertThat(findDynamicMapping(mappings), equalTo(true));
    }

    /**
     * Check that a system index descriptor correctly identifies the absence of a dynamic mapping when none are present.
     */
    public void testFindDynamicMappingsWithoutDynamicMapping() {
        String json = "{ \"foo\": { \"bar\": { \"dynamic\": false } } }";

        final Map<String, Object> mappings = XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);

        assertThat(findDynamicMapping(mappings), equalTo(false));
    }
}
