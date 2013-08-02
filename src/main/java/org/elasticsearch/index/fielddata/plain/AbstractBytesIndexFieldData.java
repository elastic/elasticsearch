/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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
package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.elasticsearch.index.mapper.FieldMapper.Names;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractBytesIndexFieldData<FD extends AtomicFieldData.WithOrdinals<ScriptDocValues.Strings>> extends AbstractIndexFieldData<FD> implements IndexFieldData.WithOrdinals<FD> {

    protected Settings frequency;
    protected Settings regex;

    protected AbstractBytesIndexFieldData(Index index, Settings indexSettings, Names fieldNames, FieldDataType fieldDataType,
            IndexFieldDataCache cache) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
        final Map<String, Settings> groups = fieldDataType.getSettings().getGroups("filter");
        frequency = groups.get("frequency");
        regex = groups.get("regex");
       
    }
    
    @Override
    public final boolean valuesOrdered() {
        return true;
    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, SortMode sortMode) {
        return new BytesRefFieldComparatorSource(this, missingValue, sortMode);
    }
    
    protected TermsEnum filter(Terms terms, AtomicReader reader) throws IOException {
        TermsEnum iterator = terms.iterator(null);
        if (iterator == null) {
            return null;
        }
        if (iterator != null && frequency != null) {
            iterator = FrequencyFilter.filter(iterator, terms, reader, frequency);
        }
        
        if (iterator != null && regex != null) {
            iterator = RegexFilter.filter(iterator, terms, reader, regex);
        }
        return iterator;
    }
    
    private static final class FrequencyFilter extends FilteredTermsEnum {

        private int minFreq;
        private int maxFreq;
        public FrequencyFilter(TermsEnum delegate, int minFreq, int maxFreq) {
            super(delegate, false);
            this.minFreq = minFreq;
            this.maxFreq = maxFreq;
        }
        
        public static TermsEnum filter(TermsEnum toFilter, Terms terms, AtomicReader reader, Settings settings) throws IOException {
            int docCount = terms.getDocCount();
            if (docCount == -1) {
                docCount = reader.maxDoc();
            }
            final double minFrequency = settings.getAsDouble("min", 0d);
            final double maxFrequency = settings.getAsDouble("max", docCount+1d);
            final double minSegmentSize = settings.getAsInt("min_segment_size", 0);
            if (minSegmentSize < docCount) {
                final int minFreq = minFrequency >= 1.0? (int) minFrequency : (int)(docCount * minFrequency);
                final int maxFreq = maxFrequency >= 1.0? (int) maxFrequency : (int)(docCount * maxFrequency);
                assert minFreq < maxFreq;
                return new FrequencyFilter(toFilter, minFreq, maxFreq);
            }
            
            return toFilter;
            
        }
        
        @Override
        protected AcceptStatus accept(BytesRef arg0) throws IOException {
            int docFreq = docFreq();
            if (docFreq >= minFreq && docFreq <= maxFreq) {
                return AcceptStatus.YES;
            }
            return AcceptStatus.NO;
        }
    }
    
    private static final class RegexFilter extends FilteredTermsEnum {

        private final Matcher matcher;
        private final CharsRef spare = new CharsRef();
        
        public RegexFilter(TermsEnum delegate, Matcher matcher) {
            super(delegate, false);
            this.matcher = matcher;
        }
        public static TermsEnum filter(TermsEnum iterator, Terms terms, AtomicReader reader, Settings regex) {
            String pattern = regex.get("pattern");
            if (pattern == null) {
                return iterator;
            } 
            Pattern p = Pattern.compile(pattern);
            return new RegexFilter(iterator, p.matcher(""));
        }
        
        @Override
        protected AcceptStatus accept(BytesRef arg0) throws IOException {
            UnicodeUtil.UTF8toUTF16(arg0, spare);
            matcher.reset(spare);
            if (matcher.matches()) {
                return AcceptStatus.YES;
            }
            return AcceptStatus.NO;
        }
    }

}
