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

package org.elasticsearch.action.admin.indices.warmer.delete;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.hamcrest.CoreMatchers.equalTo;

public class DeleteWarmerRequestTests extends ElasticsearchTestCase {

    @Test
    public void testDeleteWarmerTimeoutBwComp_Pre0906Format() throws Exception {
        DeleteWarmerRequest outRequest = new DeleteWarmerRequest("warmer1");
        outRequest.timeout(TimeValue.timeValueMillis(1000));

        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        out.setVersion(Version.V_0_90_0);
        outRequest.writeTo(out);

        ByteArrayInputStream esInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput esBuffer = new InputStreamStreamInput(esInBuffer);
        esBuffer.setVersion(Version.V_0_90_0);
        DeleteWarmerRequest inRequest = new DeleteWarmerRequest();
        inRequest.readFrom(esBuffer);

        assertThat(inRequest.name(), equalTo("warmer1"));
        //timeout is default as we don't read it from the received buffer
        assertThat(inRequest.timeout().millis(), equalTo(new DeleteWarmerRequest().timeout().millis()));

    }

    @Test
    public void testDeleteWarmerTimeoutBwComp_Post0906Format() throws Exception {
        DeleteWarmerRequest outRequest = new DeleteWarmerRequest("warmer1");
        outRequest.timeout(TimeValue.timeValueMillis(1000));

        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        out.setVersion(Version.V_0_90_6);
        outRequest.writeTo(out);

        ByteArrayInputStream esInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput esBuffer = new InputStreamStreamInput(esInBuffer);
        esBuffer.setVersion(Version.V_0_90_6);
        DeleteWarmerRequest inRequest = new DeleteWarmerRequest();
        inRequest.readFrom(esBuffer);

        assertThat(inRequest.name(), equalTo("warmer1"));
        //timeout is default as we don't read it from the received buffer
        assertThat(inRequest.timeout().millis(), equalTo(outRequest.timeout().millis()));

    }
}
