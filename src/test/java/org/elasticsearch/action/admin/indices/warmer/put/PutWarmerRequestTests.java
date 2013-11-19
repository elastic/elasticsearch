/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.action.admin.indices.warmer.put;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class PutWarmerRequestTests extends ElasticsearchTestCase {

    @Test
    public void testPutWarmerTimeoutBwComp_Pre0906Format() throws Exception {
        PutWarmerRequest outRequest = new PutWarmerRequest("warmer1");
        outRequest.timeout(TimeValue.timeValueMillis(1000));

        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        out.setVersion(Version.V_0_90_0);
        outRequest.writeTo(out);

        ByteArrayInputStream esInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput esBuffer = new InputStreamStreamInput(esInBuffer);
        esBuffer.setVersion(Version.V_0_90_0);
        PutWarmerRequest inRequest = new PutWarmerRequest();
        inRequest.readFrom(esBuffer);

        assertThat(inRequest.name(), equalTo("warmer1"));
        //timeout is default as we don't read it from the received buffer
        assertThat(inRequest.timeout().millis(), equalTo(new PutWarmerRequest().timeout().millis()));
    }

    @Test
    public void testPutWarmerTimeoutBwComp_Post0906Format() throws Exception {
        PutWarmerRequest outRequest = new PutWarmerRequest("warmer1");
        outRequest.timeout(TimeValue.timeValueMillis(1000));

        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        out.setVersion(Version.V_0_90_6);
        outRequest.writeTo(out);

        ByteArrayInputStream esInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput esBuffer = new InputStreamStreamInput(esInBuffer);
        esBuffer.setVersion(Version.V_0_90_6);
        PutWarmerRequest inRequest = new PutWarmerRequest();
        inRequest.readFrom(esBuffer);

        assertThat(inRequest.name(), equalTo("warmer1"));
        //timeout is default as we don't read it from the received buffer
        assertThat(inRequest.timeout().millis(), equalTo(outRequest.timeout().millis()));
    }

    @Test // issue 4196
    public void testThatValidationWithoutSpecifyingSearchRequestFails() {
        PutWarmerRequest putWarmerRequest = new PutWarmerRequest("foo");
        ActionRequestValidationException validationException = putWarmerRequest.validate();
        assertThat(validationException.validationErrors(), hasSize(1));
        assertThat(validationException.getMessage(), containsString("search request is missing"));
    }
}
