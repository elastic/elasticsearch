/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.equalTo;

public class RestCreateSnapshotActionTests extends ESTestCase {

    public void testFilteredRequestOmitsPassword() throws IOException {
        var request = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray("""
            {"indices": "idx", "encrypted_data": {"type": "password", "password": "secret-password"}}
            """), XContentType.JSON).build();

        assertToXContentEquivalent(new BytesArray("""
            {"indices": "idx", "encrypted_data": {"type": "password"}}
            """), new RestCreateSnapshotAction().getFilteredRequest(request).content(), XContentType.JSON);
    }

    public void testFilteredRequestOmitsMalformedBody() {
        var request = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray("""
            {"encrypted_data": {"type": "password", "password": "secret-password"
            """), XContentType.JSON).build();

        assertThat(new RestCreateSnapshotAction().getFilteredRequest(request).content().length(), equalTo(0));
    }
}
