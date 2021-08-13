/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.XContentTestUtils;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

public class ClusterUpdateSettingsRequestTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        doFromXContentTestWithRandomFields(false);
    }

    public void testFromXContentWithRandomFields() throws IOException {
        doFromXContentTestWithRandomFields(true);
    }

    private void doFromXContentTestWithRandomFields(boolean addRandomFields) throws IOException {
        final ClusterUpdateSettingsRequest request = createTestItem();
        boolean humanReadable = randomBoolean();
        final XContentType xContentType = XContentType.JSON;
        BytesReference originalBytes = toShuffledXContent(request, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);

        if (addRandomFields) {
            String unsupportedField = "unsupported_field";
            BytesReference mutated = BytesReference.bytes(XContentTestUtils.insertIntoXContent(xContentType.xContent(), originalBytes,
                    Collections.singletonList(""), () -> unsupportedField, () -> randomAlphaOfLengthBetween(3, 10)));
            XContentParseException iae = expectThrows(XContentParseException.class,
                    () -> ClusterUpdateSettingsRequest.fromXContent(createParser(xContentType.xContent(), mutated)));
            assertThat(iae.getMessage(),
                    containsString("[cluster_update_settings_request] unknown field [" + unsupportedField + "]"));
        } else {
            try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
                ClusterUpdateSettingsRequest parsedRequest = ClusterUpdateSettingsRequest.fromXContent(parser);

                assertNull(parser.nextToken());
                assertThat(parsedRequest.transientSettings(), equalTo(request.transientSettings()));
                assertThat(parsedRequest.persistentSettings(), equalTo(request.persistentSettings()));
            }
        }
    }

    private static ClusterUpdateSettingsRequest createTestItem() {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest();
        request.persistentSettings(ClusterUpdateSettingsResponseTests.randomClusterSettings(0, 2));
        request.transientSettings(ClusterUpdateSettingsResponseTests.randomClusterSettings(0, 2));
        return request;
    }
}
