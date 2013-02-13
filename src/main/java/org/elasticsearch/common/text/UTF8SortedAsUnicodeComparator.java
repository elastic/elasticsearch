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
