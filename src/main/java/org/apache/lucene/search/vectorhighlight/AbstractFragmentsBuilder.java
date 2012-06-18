/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lucene.search.vectorhighlight;

import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.highlight.Encoder;

import java.io.IOException;
import java.util.*;

/**
 * Abstract {@link FragmentsBuilder} implementation that detects whether highlight hits occurred on a field that is
 * multivalued (Basically fields that have the same name) and splits the highlight snippets according to a single field
 * boundary. This avoids that a highlight hit is shown as one hit whilst it is actually a hit on multiple fields.
 */
public abstract class AbstractFragmentsBuilder extends BaseFragmentsBuilder {

    private boolean discreteMultiValueHighlighting = true;

    protected AbstractFragmentsBuilder(){
        super();
    }

    protected AbstractFragmentsBuilder(BoundaryScanner boundaryScanner){
        super(boundaryScanner);
    }

    protected AbstractFragmentsBuilder( String[] preTags, String[] postTags ){
        super(preTags, postTags);
    }

    public AbstractFragmentsBuilder(String[] preTags, String[] postTags, BoundaryScanner bs) {
        super( preTags, postTags, bs );
    }

    public void setDiscreteMultiValueHighlighting(boolean discreteMultiValueHighlighting) {
        this.discreteMultiValueHighlighting = discreteMultiValueHighlighting;
    }

    public String[] createFragments(IndexReader reader, int docId,
                                    String fieldName, FieldFragList fieldFragList, int maxNumFragments,
                                    String[] preTags, String[] postTags, Encoder encoder) throws IOException {
        if (maxNumFragments < 0) {
            throw new IllegalArgumentException("maxNumFragments(" + maxNumFragments + ") must be positive number.");
        }

        List<String> fragments = new ArrayList<String>(maxNumFragments);
        List<FieldFragList.WeightedFragInfo> fragInfos = fieldFragList.getFragInfos();
        Field[] values = getFields(reader, docId, fieldName);
        if (values.length == 0) {
            return null;
        }

        if (discreteMultiValueHighlighting && values.length > 1) {
            fragInfos = discreteMultiValueHighlighting(fragInfos, values);
        }

        fragInfos = getWeightedFragInfoList(fragInfos);

        StringBuilder buffer = new StringBuilder();
        int[] nextValueIndex = {0};
        for (int n = 0; n < maxNumFragments && n < fragInfos.size(); n++) {
            FieldFragList.WeightedFragInfo fragInfo = fragInfos.get(n);
            fragments.add(makeFragment(buffer, nextValueIndex, values, fragInfo, preTags, postTags, encoder));
        }
        return fragments.toArray(new String[fragments.size()]);
    }

    protected List<FieldFragList.WeightedFragInfo> discreteMultiValueHighlighting(List<FieldFragList.WeightedFragInfo> fragInfos, Field[] fields) {
        Map<String, List<FieldFragList.WeightedFragInfo>> fieldNameToFragInfos = new HashMap<String, List<FieldFragList.WeightedFragInfo>>();
        for (Field field : fields) {
            fieldNameToFragInfos.put(field.name(), new ArrayList<FieldFragList.WeightedFragInfo>());
        }

        fragInfos:
        for (FieldFragList.WeightedFragInfo fragInfo : fragInfos) {
            int fieldStart;
            int fieldEnd = 0;
            for (Field field : fields) {
                if (field.stringValue().isEmpty()) {
                    fieldEnd++;
                    continue;
                }
                fieldStart = fieldEnd;
                fieldEnd += field.stringValue().length() + 1; // + 1 for going to next field with same name.

                if (fragInfo.getStartOffset() >= fieldStart && fragInfo.getEndOffset() >= fieldStart &&
                        fragInfo.getStartOffset() <= fieldEnd && fragInfo.getEndOffset() <= fieldEnd) {
                    fieldNameToFragInfos.get(field.name()).add(fragInfo);
                    continue fragInfos;
                }

                if (fragInfo.getSubInfos().isEmpty()) {
                    continue fragInfos;
                }

                FieldPhraseList.WeightedPhraseInfo.Toffs firstToffs = fragInfo.getSubInfos().get(0).getTermsOffsets().get(0);
                if (fragInfo.getStartOffset() >= fieldEnd || firstToffs.getStartOffset() >= fieldEnd) {
                    continue;
                }

                int fragStart = fieldStart;
                if (fragInfo.getStartOffset() > fieldStart && fragInfo.getStartOffset() < fieldEnd) {
                    fragStart = fragInfo.getStartOffset();
                }

                int fragEnd = fieldEnd;
                if (fragInfo.getEndOffset() > fieldStart && fragInfo.getEndOffset() < fieldEnd) {
                    fragEnd = fragInfo.getEndOffset();
                }


                List<WeightedFragInfo.SubInfo> subInfos = new ArrayList<WeightedFragInfo.SubInfo>();
                WeightedFragInfo weightedFragInfo = new WeightedFragInfo(fragStart, fragEnd, fragInfo.getTotalBoost(), subInfos);

                Iterator<FieldFragList.WeightedFragInfo.SubInfo> subInfoIterator = fragInfo.getSubInfos().iterator();
                while (subInfoIterator.hasNext()) {
                    FieldFragList.WeightedFragInfo.SubInfo subInfo = subInfoIterator.next();
                    List<FieldPhraseList.WeightedPhraseInfo.Toffs> toffsList = new ArrayList<FieldPhraseList.WeightedPhraseInfo.Toffs>();
                    Iterator<FieldPhraseList.WeightedPhraseInfo.Toffs> toffsIterator = subInfo.getTermsOffsets().iterator();
                    while (toffsIterator.hasNext()) {
                        FieldPhraseList.WeightedPhraseInfo.Toffs toffs = toffsIterator.next();
                        if (toffs.getStartOffset() >= fieldStart && toffs.getEndOffset() <= fieldEnd) {
                            toffsList.add(toffs);
                            toffsIterator.remove();
                        }
                    }
                    if (!toffsList.isEmpty()) {
                        subInfos.add(new FieldFragList.WeightedFragInfo.SubInfo(subInfo.text, toffsList, subInfo.getSeqnum()));
                    }

                    if (subInfo.getTermsOffsets().isEmpty()) {
                        subInfoIterator.remove();
                    }
                }
                fieldNameToFragInfos.get(field.name()).add(weightedFragInfo);
            }
        }

        List<FieldFragList.WeightedFragInfo> result = new ArrayList<FieldFragList.WeightedFragInfo>();
        for (List<FieldFragList.WeightedFragInfo> weightedFragInfos : fieldNameToFragInfos.values()) {
            result.addAll(weightedFragInfos);
        }
        Collections.sort(result, new Comparator<FieldFragList.WeightedFragInfo>() {

            public int compare(FieldFragList.WeightedFragInfo info1, FieldFragList.WeightedFragInfo info2) {
                return info1.getStartOffset() - info2.getStartOffset();
            }

        });

        return result;
    }

    private static class WeightedFragInfo extends FieldFragList.WeightedFragInfo {

        private final static List<FieldPhraseList.WeightedPhraseInfo> EMPTY = Collections.emptyList();

        private WeightedFragInfo(int startOffset, int endOffset, float totalBoost, List<FieldFragList.WeightedFragInfo.SubInfo> subInfos) {
            super(startOffset, endOffset, EMPTY);
            this.subInfos = subInfos;
            this.totalBoost = totalBoost;
        }
    }

}
