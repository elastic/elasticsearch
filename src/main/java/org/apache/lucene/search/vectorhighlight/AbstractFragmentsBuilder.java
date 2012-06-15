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

        if (discreteMultiValueHighlighting && values.length > fragInfos.size()) {
            Map<Field, List<FieldFragList.WeightedFragInfo>> fieldsWeightedFragInfo = new HashMap<Field, List<FieldFragList.WeightedFragInfo>>();
            int startOffset = 0;
            int endOffset = 0;
            for (Field value : values) {
                endOffset += value.stringValue().length();
                List<FieldFragList.WeightedFragInfo.SubInfo> fieldToSubInfos = new ArrayList<FieldFragList.WeightedFragInfo.SubInfo>();
                List<FieldFragList.WeightedFragInfo> fieldToWeightedFragInfos = new ArrayList<FieldFragList.WeightedFragInfo>();
                fieldsWeightedFragInfo.put(value, fieldToWeightedFragInfos);
                for (FieldFragList.WeightedFragInfo fragInfo : fragInfos) {
                    int weightedFragInfoStartOffset = startOffset;
                    if (fragInfo.getStartOffset() > startOffset && fragInfo.getStartOffset() < endOffset) {
                        weightedFragInfoStartOffset = fragInfo.getStartOffset();
                    }
                    int weightedFragInfoEndOffset = endOffset;
                    if (fragInfo.getEndOffset() > startOffset && fragInfo.getEndOffset() < endOffset) {
                        weightedFragInfoEndOffset = fragInfo.getEndOffset();
                    }

                    fieldToWeightedFragInfos.add(new WeightedFragInfo(weightedFragInfoStartOffset, weightedFragInfoEndOffset, fragInfo.getTotalBoost(), fieldToSubInfos));
                    for (FieldFragList.WeightedFragInfo.SubInfo subInfo : fragInfo.getSubInfos()) {
                        for (FieldPhraseList.WeightedPhraseInfo.Toffs toffs : subInfo.getTermsOffsets()) {
                            if (toffs.getStartOffset() >= startOffset && toffs.getEndOffset() < endOffset) {
                                fieldToSubInfos.add(subInfo);
                            }
                        }
                    }
                }
                startOffset = endOffset + 1;
            }
            fragInfos.clear();
            for (Map.Entry<Field, List<FieldFragList.WeightedFragInfo>> entry : fieldsWeightedFragInfo.entrySet()) {
                fragInfos.addAll(entry.getValue());
            }
            Collections.sort(fragInfos, new Comparator<FieldFragList.WeightedFragInfo>() {

                public int compare(FieldFragList.WeightedFragInfo info1, FieldFragList.WeightedFragInfo info2) {
                    return info1.getStartOffset() - info2.getStartOffset();
                }

            });
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

    private static class WeightedFragInfo extends FieldFragList.WeightedFragInfo {

        private final static List<FieldPhraseList.WeightedPhraseInfo> EMPTY = Collections.emptyList();

        private WeightedFragInfo(int startOffset, int endOffset, float totalBoost, List<FieldFragList.WeightedFragInfo.SubInfo> subInfos) {
            super(startOffset, endOffset, EMPTY);
            this.subInfos = subInfos;
            this.totalBoost = totalBoost;
        }
    }

}
