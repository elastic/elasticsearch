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
package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalsBuilder;
import org.elasticsearch.index.mapper.FieldMapper.Names;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractIndexOrdinalsFieldData extends AbstractIndexFieldData<AtomicOrdinalsFieldData> implements IndexOrdinalsFieldData {

    protected Settings frequency;
    protected Settings regex;
    protected final CircuitBreakerService breakerService;

    protected AbstractIndexOrdinalsFieldData(Index index, Settings indexSettings, Names fieldNames, FieldDataType fieldDataType,
                                          IndexFieldDataCache cache, CircuitBreakerService breakerService) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
        final Map<String, Settings> groups = fieldDataType.getSettings().getGroups("filter");
        frequency = groups.get("frequency");
        regex = groups.get("regex");
        this.breakerService = breakerService;
    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested) {
        return new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
    }

    @Override
    public IndexOrdinalsFieldData loadGlobal(IndexReader indexReader) {
        if (indexReader.leaves().size() <= 1) {
            // ordinals are already global
            return this;
        }
        try {
            return cache.load(indexReader, this);
        } catch (Throwable e) {
            if (e instanceof ElasticsearchException) {
                throw (ElasticsearchException) e;
            } else {
                throw new ElasticsearchException(e.getMessage(), e);
            }
        }
    }

    @Override
    public IndexOrdinalsFieldData localGlobalDirect(IndexReader indexReader) throws Exception {
        return GlobalOrdinalsBuilder.build(indexReader, this, indexSettings, breakerService, logger);
    }

    protected TermsEnum filter(Terms terms, LeafReader reader) throws IOException {
        TermsEnum iterator = terms.iterator();
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

        public static TermsEnum filter(TermsEnum toFilter, Terms terms, LeafReader reader, Settings settings) throws IOException {
            int docCount = terms.getDocCount();
            if (docCount == -1) {
                docCount = reader.maxDoc();
            }
            final double minFrequency = settings.getAsDouble("min", 0d);
            final double maxFrequency = settings.getAsDouble("max", docCount+1d);
            final double minSegmentSize = settings.getAsInt("min_segment_size", 0);
            if (minSegmentSize < docCount) {
                final int minFreq = minFrequency > 1.0? (int) minFrequency : (int)(docCount * minFrequency);
                final int maxFreq = maxFrequency > 1.0? (int) maxFrequency : (int)(docCount * maxFrequency);
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
        private final CharsRefBuilder spare = new CharsRefBuilder();

        public RegexFilter(TermsEnum delegate, Matcher matcher) {
            super(delegate, false);
            this.matcher = matcher;
        }
        public static TermsEnum filter(TermsEnum iterator, Terms terms, LeafReader reader, Settings regex) {
            String pattern = regex.get("pattern");
            if (pattern == null) {
                return iterator;
            }
            Pattern p = Pattern.compile(pattern);
            return new RegexFilter(iterator, p.matcher(""));
        }

        @Override
        protected AcceptStatus accept(BytesRef arg0) throws IOException {
            spare.copyUTF8Bytes(arg0);
            matcher.reset(spare.get());
            if (matcher.matches()) {
                return AcceptStatus.YES;
            }
            return AcceptStatus.NO;
        }
    }

}
