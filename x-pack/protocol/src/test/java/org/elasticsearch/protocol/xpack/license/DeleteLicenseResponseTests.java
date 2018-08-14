/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.protocol.xpack.license;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

public class DeleteLicenseResponseTests extends AbstractStreamableXContentTestCase<DeleteLicenseResponse> {

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected DeleteLicenseResponse createTestInstance() {
        return new DeleteLicenseResponse(randomBoolean());
    }

    @Override
    protected DeleteLicenseResponse doParseInstance(XContentParser parser) {
        return DeleteLicenseResponse.fromXContent(parser);
    }

    @Override
    protected DeleteLicenseResponse createBlankInstance() {
        return new DeleteLicenseResponse();
    }

    @Override
    protected DeleteLicenseResponse mutateInstance(DeleteLicenseResponse response) {
        return new DeleteLicenseResponse(!response.isAcknowledged());
    }
}
