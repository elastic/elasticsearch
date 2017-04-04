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
package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class UidTests extends ESTestCase {
    public void testCreateAndSplitId() {
        BytesRef createUid = Uid.createUidAsBytes("foo", "bar");
        BytesRef[] splitUidIntoTypeAndId = splitUidIntoTypeAndId(createUid);
        assertThat("foo", equalTo(splitUidIntoTypeAndId[0].utf8ToString()));
        assertThat("bar", equalTo(splitUidIntoTypeAndId[1].utf8ToString()));
        // split also with an offset
        BytesRef ref = new BytesRef(createUid.length+10);
        ref.offset = 9;
        ref.length = createUid.length;
        System.arraycopy(createUid.bytes, createUid.offset, ref.bytes, ref.offset, ref.length);
        splitUidIntoTypeAndId = splitUidIntoTypeAndId(ref);
        assertThat("foo", equalTo(splitUidIntoTypeAndId[0].utf8ToString()));
        assertThat("bar", equalTo(splitUidIntoTypeAndId[1].utf8ToString()));
    }

    public static BytesRef[] splitUidIntoTypeAndId(BytesRef uid) {
        int loc = -1;
        final int limit = uid.offset + uid.length;
        for (int i = uid.offset; i < limit; i++) {
            if (uid.bytes[i] == Uid.DELIMITER_BYTE) { // 0x23 is equal to '#'
                loc = i;
                break;
            }
        }

        if (loc == -1) {
            return null;
        }

        int idStart = loc + 1;
        return new BytesRef[] {
            new BytesRef(uid.bytes, uid.offset, loc - uid.offset),
            new BytesRef(uid.bytes, idStart, limit - idStart)
        };
    }
}
