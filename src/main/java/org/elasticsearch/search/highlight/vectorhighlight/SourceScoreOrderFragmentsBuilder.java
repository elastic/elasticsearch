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

package org.elasticsearch.search.highlight.vectorhighlight;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.ngram.NGramTokenizerFactory;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.highlight.Encoder;
import org.apache.lucene.search.vectorhighlight.BoundaryScanner;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo.SubInfo;
import org.apache.lucene.search.vectorhighlight.ScoreOrderFragmentsBuilder;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

/**
 *
 */
public class SourceScoreOrderFragmentsBuilder extends ScoreOrderFragmentsBuilder {

    private final FieldMapper<?> mapper;

    private final SearchContext searchContext;

    public SourceScoreOrderFragmentsBuilder(FieldMapper<?> mapper, SearchContext searchContext,
                                            String[] preTags, String[] postTags, BoundaryScanner boundaryScanner) {
        super(preTags, postTags, boundaryScanner);
        this.mapper = mapper;
        this.searchContext = searchContext;
    }

    @Override
    protected Field[] getFields(IndexReader reader, int docId, String fieldName) throws IOException {
        // we know its low level reader, and matching docId, since that's how we call the highlighter with
        SearchLookup lookup = searchContext.lookup();
        lookup.setNextReader((AtomicReaderContext) reader.getContext());
        lookup.setNextDocId(docId);

        List<Object> values = lookup.source().extractRawValues(mapper.names().sourcePath());
        Field[] fields = new Field[values.size()];
        for (int i = 0; i < values.size(); i++) {
            fields[i] = new Field(mapper.names().indexName(), values.get(i).toString(), TextField.TYPE_NOT_STORED);
        }
        return fields;
    }
    
    protected String makeFragment( StringBuilder buffer, int[] index, Field[] values, WeightedFragInfo fragInfo,
            String[] preTags, String[] postTags, Encoder encoder ){
        return super.makeFragment(buffer, index, values, FragmentBuilderHelper.fixWeightedFragInfo(mapper, values, fragInfo), preTags, postTags, encoder);
    }
}
