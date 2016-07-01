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

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.util.Comparator;

// LUCENE 4 UPGRADE: Is this the right way of comparing bytesreferences inside Text instances?
// Copied from Lucene's BytesRef comparator
public final class UTF8SortedAsUnicodeComparator implements Comparator<BytesReference> {
    public final static Comparator<BytesReference> utf8SortedAsUnicodeSortOrder = new UTF8SortedAsUnicodeComparator();
    // Only singleton
    private UTF8SortedAsUnicodeComparator() {}
    @Override
    public int compare(BytesReference a, BytesReference b) {
        try {
            // we use the iterators since it's a 0-copy comparison where possible!
            final BytesRefIterator aIter = a.iterator();
            final BytesRefIterator bIter = b.iterator();
            final long lengthToCompare = Math.min(a.length(), b.length());
            BytesRef aRef = aIter.next();
            BytesRef bRef = bIter.next();
            if (aRef != null && bRef != null) { // do we have any data?
                if (aRef.length == a.length() && bRef.length == b.length()) { // is it only one array slice we are comparing?
                    return aRef.compareTo(bRef);
                } else {
                    for (int i = 0; i < lengthToCompare;) {
                        if (aRef.length == 0) {
                            aRef = aIter.next();
                        }
                        if (bRef.length == 0) {
                            bRef = bIter.next();
                        }
                        final int aLength = aRef.length;
                        final int bLength = bRef.length;
                        final int length = Math.min(aLength, bLength); // shrink to the same length and use the fast compare in lucene
                        aRef.length = bRef.length = length;
                        // now we move to the fast comparison - this is the hot part of the loop
                        int diff = aRef.compareTo(bRef);
                        aRef.length = aLength;
                        bRef.length = bLength;

                        if (diff != 0) {
                            return diff;
                        }
                        advance(aRef, length);
                        advance(bRef, length);
                        i += length;
                    }
                }
            }
            // One is a prefix of the other, or, they are equal:
            return a.length() - b.length();
        } catch (IOException ex) {
            throw new AssertionError("can not happen", ex);
        }
    }

    private static final void advance(BytesRef ref, int length) {
        assert ref.length >= length : " ref.length: " + ref.length + " length: " + length;
        assert ref.offset+length < ref.bytes.length || (ref.offset+length == ref.bytes.length && ref.length-length == 0)
            : "offset: " + ref.offset + " ref.bytes.length: " + ref.bytes.length + " length: " + length + " ref.length: " + ref.length;
        ref.length -= length;
        ref.offset += length;
    }

}
