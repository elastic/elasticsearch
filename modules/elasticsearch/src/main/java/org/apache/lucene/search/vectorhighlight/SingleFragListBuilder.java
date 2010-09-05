/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.apache.lucene.search.vectorhighlight;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Copy from lucene trunk:
 * http://svn.apache.org/viewvc/lucene/dev/trunk/lucene/contrib/highlighter/src/java/org/apache/lucene/search/vectorhighlight/SingleFragListBuilder.java
 * This class in not available in 3.0.2 release yet.
 */
// LUCENE MONITOR
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
