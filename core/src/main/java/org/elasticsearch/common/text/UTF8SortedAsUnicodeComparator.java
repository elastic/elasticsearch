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
package org.elasticsearch.common.text;

import org.elasticsearch.common.bytes.BytesReference;

import java.util.Comparator;

// LUCENE 4 UPGRADE: Is this the right way of comparing bytesreferences inside Text instances?
// Copied from Lucene's BytesRef comparator
public class UTF8SortedAsUnicodeComparator implements Comparator<BytesReference> {

    public final static Comparator<BytesReference> utf8SortedAsUnicodeSortOrder = new UTF8SortedAsUnicodeComparator();

    // Only singleton
    private UTF8SortedAsUnicodeComparator() {
    }

    @Override
    public int compare(BytesReference a, BytesReference b) {
        if (a.hasArray() && b.hasArray()) {
            final byte[] aBytes = a.array();
            int aUpto = a.arrayOffset();
            final byte[] bBytes = b.array();
            int bUpto = b.arrayOffset();

            final int aStop = aUpto + Math.min(a.length(), b.length());
            while (aUpto < aStop) {
                int aByte = aBytes[aUpto++] & 0xff;
                int bByte = bBytes[bUpto++] & 0xff;

                int diff = aByte - bByte;
                if (diff != 0) {
                    return diff;
                }
            }

            // One is a prefix of the other, or, they are equal:
            return a.length() - b.length();
        } else {
            final byte[] aBytes = a.toBytes();
            int aUpto = 0;
            final byte[] bBytes = b.toBytes();
            int bUpto = 0;

            final int aStop = aUpto + Math.min(a.length(), b.length());
            while (aUpto < aStop) {
                int aByte = aBytes[aUpto++] & 0xff;
                int bByte = bBytes[bUpto++] & 0xff;

                int diff = aByte - bByte;
                if (diff != 0) {
                    return diff;
                }
            }

            // One is a prefix of the other, or, they are equal:
            return a.length() - b.length();
        }
    }
}
