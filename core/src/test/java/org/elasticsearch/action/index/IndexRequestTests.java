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
package org.elasticsearch.action.index;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

/**
  */
public class IndexRequestTests extends ESTestCase {

    @Test
    public void testIndexRequestOpTypeFromString() throws Exception {
        String create = "create";
        String index = "index";
        String createUpper = "CREATE";
        String indexUpper = "INDEX";

        assertThat(IndexRequest.OpType.fromString(create), equalTo(IndexRequest.OpType.CREATE));
        assertThat(IndexRequest.OpType.fromString(index), equalTo(IndexRequest.OpType.INDEX));
        assertThat(IndexRequest.OpType.fromString(createUpper), equalTo(IndexRequest.OpType.CREATE));
        assertThat(IndexRequest.OpType.fromString(indexUpper), equalTo(IndexRequest.OpType.INDEX));
    }

    @Test(expected= IllegalArgumentException.class)
    public void testReadBogusString(){
        String foobar = "foobar";
        IndexRequest.OpType.fromString(foobar);
    }

    public void testSetTTLAsTimeValue() {
        IndexRequest indexRequest = new IndexRequest();
        TimeValue ttl = TimeValue.parseTimeValue(randomTimeValue(), null, "ttl");
        indexRequest.ttl(ttl);
        assertThat(indexRequest.ttl(), equalTo(ttl));
    }

    public void testSetTTLAsString() {
        IndexRequest indexRequest = new IndexRequest();
        String ttlAsString = randomTimeValue();
        TimeValue ttl = TimeValue.parseTimeValue(ttlAsString, null, "ttl");
        indexRequest.ttl(ttlAsString);
        assertThat(indexRequest.ttl(), equalTo(ttl));
    }

    public void testSetTTLAsLong() {
        IndexRequest indexRequest = new IndexRequest();
        String ttlAsString = randomTimeValue();
        TimeValue ttl = TimeValue.parseTimeValue(ttlAsString, null, "ttl");
        indexRequest.ttl(ttl.millis());
        assertThat(indexRequest.ttl(), equalTo(ttl));
    }

    public void testValidateTTL() {
        IndexRequest indexRequest = new IndexRequest("index", "type");
        if (randomBoolean()) {
            indexRequest.ttl(randomIntBetween(Integer.MIN_VALUE, -1));
        } else {
            if (randomBoolean()) {
                indexRequest.ttl(new TimeValue(randomIntBetween(Integer.MIN_VALUE, -1)));
            } else {
                indexRequest.ttl(randomIntBetween(Integer.MIN_VALUE, -1) + "ms");
            }
        }
        ActionRequestValidationException validate = indexRequest.validate();
        assertThat(validate, notNullValue());
        assertThat(validate.getMessage(), containsString("ttl must not be negative"));
    }

    public void testTTLSerialization() throws Exception {
        IndexRequest indexRequest = new IndexRequest("index", "type");
        TimeValue expectedTTL = null;
        if (randomBoolean()) {
            String randomTimeValue = randomTimeValue();
            expectedTTL = TimeValue.parseTimeValue(randomTimeValue, null, "ttl");
            if (randomBoolean()) {
                indexRequest.ttl(randomTimeValue);
            } else {
                if (randomBoolean()) {
                    indexRequest.ttl(expectedTTL);
                } else {
                    indexRequest.ttl(expectedTTL.millis());
                }
            }
        }

        Version version = VersionUtils.randomVersion(random());
        BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(version);
        indexRequest.writeTo(out);

        StreamInput in = StreamInput.wrap(out.bytes());
        in.setVersion(version);
        IndexRequest newIndexRequest = new IndexRequest();
        newIndexRequest.readFrom(in);
        assertThat(newIndexRequest.ttl(), equalTo(expectedTTL));
    }
}
