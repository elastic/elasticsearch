package org.elasticsearch.common.lucene.search.vectorhighlight;

import gnu.trove.set.hash.TCharHashSet;
import org.apache.lucene.search.vectorhighlight.BoundaryScanner;

/**
 * A copy of Lucene {@link org.apache.lucene.search.vectorhighlight.XSimpleBoundaryScanner}.
 * <p/>
 * Uses specialized char set to lookup boundary, and fixes a problem with start offset in the
 * beginning of the text: https://issues.apache.org/jira/browse/LUCENE-3697 (which has a problem
 * with multiple empty fields to highlight...).
 */
public class SimpleBoundaryScanner2 implements BoundaryScanner {

    public static final int DEFAULT_MAX_SCAN = 20;
    public static final char[] DEFAULT_BOUNDARY_CHARS = {'.', ',', '!', '?', ' ', '\t', '\n'};

    public static final SimpleBoundaryScanner2 DEFAULT = new SimpleBoundaryScanner2();

    public int maxScan;
    public TCharHashSet boundaryChars;

    public SimpleBoundaryScanner2() {
        this(DEFAULT_MAX_SCAN, DEFAULT_BOUNDARY_CHARS);
    }

    public SimpleBoundaryScanner2(int maxScan, char[] boundaryChars) {
        this.maxScan = maxScan;
        this.boundaryChars = new TCharHashSet(boundaryChars);
    }

    public int findStartOffset(StringBuilder buffer, int start) {
        // avoid illegal start offset
        if (start > buffer.length() || start < 1) return start;
        int offset, count = maxScan;
        for (offset = start; offset > 0 && count > 0; count--) {
            // found?
            if (boundaryChars.contains(buffer.charAt(offset - 1))) return offset;
            offset--;
        }
        // LUCENE-3697
        if (offset == 0) {
            return 0;
        }
        // not found
        return start;
    }

    public int findEndOffset(StringBuilder buffer, int start) {
        // avoid illegal start offset
        if (start > buffer.length() || start < 0) return start;
        int offset, count = maxScan;
        //for( offset = start; offset <= buffer.length() && count > 0; count-- ){
        for (offset = start; offset < buffer.length() && count > 0; count--) {
            // found?
            if (boundaryChars.contains(buffer.charAt(offset))) return offset;
            offset++;
        }
        // not found
        return start;
    }
}
