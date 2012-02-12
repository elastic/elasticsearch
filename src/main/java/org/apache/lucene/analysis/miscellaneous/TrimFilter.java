package org.apache.lucene.analysis.miscellaneous;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;

import java.io.IOException;

/**
 */
// LUCENE MONITOR: Next version of Lucene (4.0) will have this as part of the analyzers module
public final class TrimFilter extends TokenFilter {

    final boolean updateOffsets;
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);


    public TrimFilter(TokenStream in, boolean updateOffsets) {
        super(in);
        this.updateOffsets = updateOffsets;
    }

    @Override
    public boolean incrementToken() throws IOException {
        if (!input.incrementToken()) return false;

        char[] termBuffer = termAtt.buffer();
        int len = termAtt.length();
        //TODO: Is this the right behavior or should we return false?  Currently, "  ", returns true, so I think this should
        //also return true
        if (len == 0) {
            return true;
        }
        int start = 0;
        int end = 0;
        int endOff = 0;

        // eat the first characters
        //QUESTION: Should we use Character.isWhitespace() instead?
        for (start = 0; start < len && termBuffer[start] <= ' '; start++) {
        }
        // eat the end characters
        for (end = len; end >= start && termBuffer[end - 1] <= ' '; end--) {
            endOff++;
        }
        if (start > 0 || end < len) {
            if (start < end) {
                termAtt.copyBuffer(termBuffer, start, (end - start));
            } else {
                termAtt.setEmpty();
            }
            if (updateOffsets) {
                int newStart = offsetAtt.startOffset() + start;
                int newEnd = offsetAtt.endOffset() - (start < end ? endOff : 0);
                offsetAtt.setOffset(newStart, newEnd);
            }
        }

        return true;
    }
}
