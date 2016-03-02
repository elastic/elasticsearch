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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 */
public class IndexRequestTests extends ESTestCase {
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

    public void testReadBogusString() {
        try {
            IndexRequest.OpType.fromString("foobar");
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("opType [foobar] not allowed"));
        }
    }

    public void testCreateOperationRejectsVersions() {
        Set<VersionType> allButInternalSet = new HashSet<>(Arrays.asList(VersionType.values()));
        allButInternalSet.remove(VersionType.INTERNAL);
        VersionType[] allButInternal = allButInternalSet.toArray(new VersionType[]{});
        IndexRequest request = new IndexRequest("index", "type", "1");
        request.opType(IndexRequest.OpType.CREATE);
        request.versionType(randomFrom(allButInternal));
        assertThat(request.validate().validationErrors(), not(empty()));

        request.versionType(VersionType.INTERNAL);
        request.version(randomIntBetween(0, Integer.MAX_VALUE));
        assertThat(request.validate().validationErrors(), not(empty()));
    }

    public void testIndexingRejectsLongIds() {
        String id = randomAsciiOfLength(511);
        IndexRequest request = new IndexRequest("index", "type", id);
        request.source("{}");
        ActionRequestValidationException validate = request.validate();
        assertNull(validate);

        id = randomAsciiOfLength(512);
        request = new IndexRequest("index", "type", id);
        request.source("{}");
        validate = request.validate();
        assertNull(validate);

        id = randomAsciiOfLength(513);
        request = new IndexRequest("index", "type", id);
        request.source("{}");
        validate = request.validate();
        assertThat(validate, notNullValue());
        assertThat(validate.getMessage(),
                containsString("id is too long, must be no longer than 512 bytes but was: 513"));
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
}
