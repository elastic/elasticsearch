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
package org.elasticsearch.search.aggregations.bucket.terms.support;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;

import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSource.Bytes.WithOrdinals;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Defines the include/exclude regular expression filtering for string terms aggregation. In this filtering logic,
 * exclusion has precedence, where the {@code include} is evaluated first and then the {@code exclude}.
 */
public class IncludeExclude {

    // The includeValue and excludeValue ByteRefs which are the result of the parsing
    // process are converted into a LongFilter when used on numeric fields
    // in the index.
    public static class LongFilter {
        private LongSet valids;
        private LongSet invalids;

        private LongFilter(int numValids, int numInvalids) {
            if (numValids > 0) {
                valids = new LongHashSet(numValids);
            }
            if (numInvalids > 0) {
                invalids = new LongHashSet(numInvalids);
            }
        }

        public boolean accept(long value) {
            return ((valids == null) || (valids.contains(value))) && ((invalids == null) || (!invalids.contains(value)));
        }

        private void addAccept(long val) {
            valids.add(val);
        }

        private void addReject(long val) {
            invalids.add(val);
        }
    }

    // Only used for the 'map' execution mode (ie. scripts)
    public abstract static class StringFilter {
        public abstract boolean accept(BytesRef value);
    }

    static class AutomatonBackedStringFilter extends StringFilter {

        private final ByteRunAutomaton runAutomaton;

        private AutomatonBackedStringFilter(Automaton automaton) {
            this.runAutomaton = new ByteRunAutomaton(automaton);
        }

        /**
         * Returns whether the given value is accepted based on the {@code include} & {@code exclude} patterns.
         */
        @Override
        public boolean accept(BytesRef value) {
            return runAutomaton.run(value.bytes, value.offset, value.length);
        }
    }

    static class TermListBackedStringFilter extends StringFilter {

        private final Set<BytesRef> valids;
        private final Set<BytesRef> invalids;

        public TermListBackedStringFilter(Set<BytesRef> includeValues, Set<BytesRef> excludeValues) {
            this.valids = includeValues;
            this.invalids = excludeValues;
        }

        /**
         * Returns whether the given value is accepted based on the
         * {@code include} & {@code exclude} sets.
         */
        @Override
        public boolean accept(BytesRef value) {
            return ((valids == null) || (valids.contains(value))) && ((invalids == null) || (!invalids.contains(value)));
        }
    }

    public static abstract class OrdinalsFilter {
        public abstract LongBitSet acceptedGlobalOrdinals(RandomAccessOrds globalOrdinals, ValuesSource.Bytes.WithOrdinals valueSource) throws IOException;
        
    }

    static class AutomatonBackedOrdinalsFilter extends OrdinalsFilter {

        private final CompiledAutomaton compiled;

        private AutomatonBackedOrdinalsFilter(Automaton automaton) {
            this.compiled = new CompiledAutomaton(automaton);
        }

        /**
         * Computes which global ordinals are accepted by this IncludeExclude instance.
         * 
         */
        @Override
        public LongBitSet acceptedGlobalOrdinals(RandomAccessOrds globalOrdinals, ValuesSource.Bytes.WithOrdinals valueSource) throws IOException {
            LongBitSet acceptedGlobalOrdinals = new LongBitSet(globalOrdinals.getValueCount());
            TermsEnum globalTermsEnum;
            Terms globalTerms = new DocValuesTerms(globalOrdinals);
            // TODO: specialize based on compiled.type: for ALL and prefixes (sinkState >= 0 ) we can avoid i/o and just set bits.
            globalTermsEnum = compiled.getTermsEnum(globalTerms);
            for (BytesRef term = globalTermsEnum.next(); term != null; term = globalTermsEnum.next()) {
                acceptedGlobalOrdinals.set(globalTermsEnum.ord());
            }
            return acceptedGlobalOrdinals;
        }

    }
    
    static class TermListBackedOrdinalsFilter extends OrdinalsFilter {

        private final SortedSet<BytesRef> includeValues;
        private final SortedSet<BytesRef> excludeValues;

        public TermListBackedOrdinalsFilter(SortedSet<BytesRef> includeValues, SortedSet<BytesRef> excludeValues) {
            this.includeValues = includeValues;
            this.excludeValues = excludeValues;
        }

        @Override
        public LongBitSet acceptedGlobalOrdinals(RandomAccessOrds globalOrdinals, WithOrdinals valueSource) throws IOException {
            LongBitSet acceptedGlobalOrdinals = new LongBitSet(globalOrdinals.getValueCount());
            if(includeValues!=null){
                for (BytesRef term : includeValues) {
                    long ord = globalOrdinals.lookupTerm(term);
                    if (ord >= 0) {
                        acceptedGlobalOrdinals.set(ord);
                    }
                }                
            } else {
                // default to all terms being acceptable
                acceptedGlobalOrdinals.set(0, acceptedGlobalOrdinals.length());
            }
            if (excludeValues != null) {
                for (BytesRef term : excludeValues) {
                    long ord = globalOrdinals.lookupTerm(term);
                    if (ord >= 0) {
                        acceptedGlobalOrdinals.clear(ord);
                    }
                }
            }
            return acceptedGlobalOrdinals;
        }

    }

    private final RegExp include, exclude;
    private final SortedSet<BytesRef> includeValues, excludeValues;

    /**
     * @param include   The regular expression pattern for the terms to be included
     * @param exclude   The regular expression pattern for the terms to be excluded
     */
    public IncludeExclude(RegExp include, RegExp exclude) {
        if (include == null && exclude == null) {
            throw new IllegalArgumentException();
        }
        this.include = include;
        this.exclude = exclude;
        this.includeValues = null;
        this.excludeValues = null;
    }

    /**
     * @param includeValues   The terms to be included
     * @param excludeValues   The terms to be excluded
     */
    public IncludeExclude(SortedSet<BytesRef> includeValues, SortedSet<BytesRef> excludeValues) {
        if (includeValues == null && excludeValues == null) {
            throw new IllegalArgumentException();
        }
        this.include = null;
        this.exclude = null;
        this.includeValues = includeValues;
        this.excludeValues = excludeValues;
    }

    /**
     * Terms adapter around doc values.
     */
    private static class DocValuesTerms extends Terms {

        private final SortedSetDocValues values;

        DocValuesTerms(SortedSetDocValues values) {
            this.values = values;
        }

        @Override
        public TermsEnum iterator() throws IOException {
            return values.termsEnum();
        }

        @Override
        public long size() throws IOException {
            return -1;
        }

        @Override
        public long getSumTotalTermFreq() throws IOException {
            return -1;
        }

        @Override
        public long getSumDocFreq() throws IOException {
            return -1;
        }

        @Override
        public int getDocCount() throws IOException {
            return -1;
        }

        @Override
        public boolean hasFreqs() {
            return false;
        }

        @Override
        public boolean hasOffsets() {
            return false;
        }

        @Override
        public boolean hasPositions() {
            return false;
        }

        @Override
        public boolean hasPayloads() {
            return false;
        }

    }



    public static class Parser {

        String include = null;
        String exclude = null;
        SortedSet<BytesRef> includeValues;
        SortedSet<BytesRef> excludeValues;

        public boolean token(String currentFieldName, XContentParser.Token token, XContentParser parser) throws IOException {

            if (token == XContentParser.Token.VALUE_STRING) {
                if ("include".equals(currentFieldName)) {
                    include = parser.text();
                } else if ("exclude".equals(currentFieldName)) {
                    exclude = parser.text();
                } else {
                    return false;
                }
                return true;
            }

            if (token == XContentParser.Token.START_ARRAY) {
                if ("include".equals(currentFieldName)) {
                     includeValues = new TreeSet<>(parseArrayToSet(parser));
                     return true;
                }
                if ("exclude".equals(currentFieldName)) {
                      excludeValues = new TreeSet<>(parseArrayToSet(parser));
                      return true;
                }
                return false;
            }

            if (token == XContentParser.Token.START_OBJECT) {
                if ("include".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            if ("pattern".equals(currentFieldName)) {
                                include = parser.text();
                            }
                        }
                    }
                } else if ("exclude".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            if ("pattern".equals(currentFieldName)) {
                                exclude = parser.text();
                            }
                        }
                    }
                } else {
                    return false;
                }
                return true;
            }

            return false;
        }
        private Set<BytesRef> parseArrayToSet(XContentParser parser) throws IOException {
            final Set<BytesRef> set = new HashSet<>();
            if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
                throw new ElasticsearchParseException("Missing start of array in include/exclude clause");
            }
            while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                if (!parser.currentToken().isValue()) {
                    throw new ElasticsearchParseException("Array elements in include/exclude clauses should be string values");
                }
                set.add(new BytesRef(parser.text()));
            }
            return set;
        }

        public IncludeExclude includeExclude() {
            RegExp includePattern =  include != null ? new RegExp(include) : null;
            RegExp excludePattern = exclude != null ? new RegExp(exclude) : null;
            if (includePattern != null || excludePattern != null) {
                if (includeValues != null || excludeValues != null) {
                    throw new IllegalArgumentException("Can only use regular expression include/exclude or a set of values, not both");
                }
                return new IncludeExclude(includePattern, excludePattern);
            } else if (includeValues != null || excludeValues != null) {
                return new IncludeExclude(includeValues, excludeValues);
            } else {
                return null;
            }
        }
    }

    public boolean isRegexBased() {
        return include != null || exclude != null;
    }

    private Automaton toAutomaton() {
        Automaton a = null;
        if (include != null) {
            a = include.toAutomaton();
        } else if (includeValues != null) {
            a = Automata.makeStringUnion(includeValues);
        } else {
            a = Automata.makeAnyString();
        }
        if (exclude != null) {
            a = Operations.minus(a, exclude.toAutomaton(), Operations.DEFAULT_MAX_DETERMINIZED_STATES);
        } else if (excludeValues != null) {
            a = Operations.minus(a, Automata.makeStringUnion(excludeValues), Operations.DEFAULT_MAX_DETERMINIZED_STATES);
        }
        return a;
    }

    public StringFilter convertToStringFilter() {
        if (isRegexBased()) {
            return new AutomatonBackedStringFilter(toAutomaton());
        }
        return new TermListBackedStringFilter(includeValues, excludeValues);
    }

    public OrdinalsFilter convertToOrdinalsFilter() {

        if (isRegexBased()) {
            return new AutomatonBackedOrdinalsFilter(toAutomaton());
        }
        return new TermListBackedOrdinalsFilter(includeValues, excludeValues);
    }

    public LongFilter convertToLongFilter() {
        int numValids = includeValues == null ? 0 : includeValues.size();
        int numInvalids = excludeValues == null ? 0 : excludeValues.size();
        LongFilter result = new LongFilter(numValids, numInvalids);
        if (includeValues != null) {
            for (BytesRef val : includeValues) {
                result.addAccept(Long.parseLong(val.utf8ToString()));
            }
        }
        if (excludeValues != null) {
            for (BytesRef val : excludeValues) {
                result.addReject(Long.parseLong(val.utf8ToString()));
            }
        }
        return result;
    }

    public LongFilter convertToDoubleFilter() {
        int numValids = includeValues == null ? 0 : includeValues.size();
        int numInvalids = excludeValues == null ? 0 : excludeValues.size();
        LongFilter result = new LongFilter(numValids, numInvalids);
        if (includeValues != null) {
            for (BytesRef val : includeValues) {
                double dval=Double.parseDouble(val.utf8ToString());
                result.addAccept( NumericUtils.doubleToSortableLong(dval));
            }
        }
        if (excludeValues != null) {
            for (BytesRef val : excludeValues) {
                double dval=Double.parseDouble(val.utf8ToString());
                result.addReject( NumericUtils.doubleToSortableLong(dval));
            }
        }
        return result;
    }

}
