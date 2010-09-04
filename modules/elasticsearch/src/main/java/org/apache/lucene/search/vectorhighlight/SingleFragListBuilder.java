package org.apache.lucene.search.vectorhighlight;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Copy from lucene trunk:
 * http://svn.apache.org/viewvc/lucene/dev/trunk/lucene/contrib/highlighter/src/java/org/apache/lucene/search/vectorhighlight/SingleFragListBuilder.java
 * This class in not available in 3.0.2 release yet.
 */
public class SingleFragListBuilder implements FragListBuilder {

    @Override public FieldFragList createFieldFragList(FieldPhraseList fieldPhraseList, int fragCharSize) {
        FieldFragList ffl = new FieldFragList(fragCharSize);

        List<FieldPhraseList.WeightedPhraseInfo> wpil = new ArrayList<FieldPhraseList.WeightedPhraseInfo>();
        Iterator<FieldPhraseList.WeightedPhraseInfo> ite = fieldPhraseList.phraseList.iterator();
        FieldPhraseList.WeightedPhraseInfo phraseInfo = null;
        while (true) {
            if (!ite.hasNext()) break;
            phraseInfo = ite.next();
            if (phraseInfo == null) break;

            wpil.add(phraseInfo);
        }
        if (wpil.size() > 0)
            ffl.add(0, Integer.MAX_VALUE, wpil);
        return ffl;
    }
}
