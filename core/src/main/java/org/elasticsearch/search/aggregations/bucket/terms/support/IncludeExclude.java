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

import com.carrotsearch.hppc.BitMixer;
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
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Defines the include/exclude regular expression filtering for string terms aggregation. In this filtering logic,
 * exclusion has precedence, where the {@code include} is evaluated first and then the {@code exclude}.
 */
public class IncludeExclude implements Writeable, ToXContent {
    private static final ParseField INCLUDE_FIELD = new ParseField("include");
    private static final ParseField EXCLUDE_FIELD = new ParseField("exclude");
    private static final ParseField PATTERN_FIELD = new ParseField("pattern");
    private static final ParseField PARTITION_FIELD = new ParseField("partition");
    private static final ParseField NUM_PARTITIONS_FIELD = new ParseField("num_partitions");

    // The includeValue and excludeValue ByteRefs which are the result of the parsing
    // process are converted into a LongFilter when used on numeric fields
    // in the index.
    public abstract static class LongFilter {
        public abstract boolean accept(long value);
   
    }
    
    public class PartitionedLongFilter extends LongFilter {
        private final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);

        @Override
        public boolean accept(long value) {
            // hash the value to keep even distributions
            final long hashCode = BitMixer.mix64(value);
            return Math.floorMod(hashCode, incNumPartitions) == incZeroBasedPartition;
        }
    }
    
    
    public static class SetBackedLongFilter extends LongFilter {
        private LongSet valids;
        private LongSet invalids;

        private SetBackedLongFilter(int numValids, int numInvalids) {
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

    class PartitionedStringFilter extends StringFilter {
        @Override
        public boolean accept(BytesRef value) {
            return Math.floorMod(value.hashCode(), incNumPartitions) == incZeroBasedPartition;
        }
    }    
    
    static class AutomatonBackedStringFilter extends StringFilter {

        private final ByteRunAutomaton runAutomaton;

        private AutomatonBackedStringFilter(Automaton automaton) {
            this.runAutomaton = new ByteRunAutomaton(automaton);
        }

        /**
         * Returns whether the given value is accepted based on the {@code include} &amp; {@code exclude} patterns.
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
         * {@code include} &amp; {@code exclude} sets.
         */
        @Override
        public boolean accept(BytesRef value) {
            return ((valids == null) || (valids.contains(value))) && ((invalids == null) || (!invalids.contains(value)));
        }
    }

    public abstract static class OrdinalsFilter {
        public abstract LongBitSet acceptedGlobalOrdinals(RandomAccessOrds globalOrdinals) throws IOException;

    }

    class PartitionedOrdinalsFilter extends OrdinalsFilter {

        @Override
        public LongBitSet acceptedGlobalOrdinals(RandomAccessOrds globalOrdinals) throws IOException {
            final long numOrds = globalOrdinals.getValueCount();
            final LongBitSet acceptedGlobalOrdinals = new LongBitSet(numOrds);
            final TermsEnum termEnum = globalOrdinals.termsEnum();

            BytesRef term = termEnum.next();
            while (term != null) {
                if (Math.floorMod(term.hashCode(), incNumPartitions) == incZeroBasedPartition) {
                    acceptedGlobalOrdinals.set(termEnum.ord());
                }
                term = termEnum.next();
            }
            return acceptedGlobalOrdinals;
        }
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
        public LongBitSet acceptedGlobalOrdinals(RandomAccessOrds globalOrdinals)
                throws IOException {
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
        public LongBitSet acceptedGlobalOrdinals(RandomAccessOrds globalOrdinals) throws IOException {
            LongBitSet acceptedGlobalOrdinals = new LongBitSet(globalOrdinals.getValueCount());
            if (includeValues != null) {
                for (BytesRef term : includeValues) {
                    long ord = globalOrdinals.lookupTerm(term);
                    if (ord >= 0) {
                        acceptedGlobalOrdinals.set(ord);
                    }
                }
            } else if (acceptedGlobalOrdinals.length() > 0) {
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
    private final int incZeroBasedPartition;
    private final int incNumPartitions;

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
        this.incZeroBasedPartition = 0;
        this.incNumPartitions = 0;
    }

    public IncludeExclude(String include, String exclude) {
        this(include == null ? null : new RegExp(include), exclude == null ? null : new RegExp(exclude));
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
        this.incZeroBasedPartition = 0;
        this.incNumPartitions = 0;        
        this.includeValues = includeValues;
        this.excludeValues = excludeValues;
    }

    public IncludeExclude(String[] includeValues, String[] excludeValues) {
        this(convertToBytesRefSet(includeValues), convertToBytesRefSet(excludeValues));
    }

    public IncludeExclude(double[] includeValues, double[] excludeValues) {
        this(convertToBytesRefSet(includeValues), convertToBytesRefSet(excludeValues));
    }

    public IncludeExclude(long[] includeValues, long[] excludeValues) {
        this(convertToBytesRefSet(includeValues), convertToBytesRefSet(excludeValues));
    }

    public IncludeExclude(int partition, int numPartitions) {
        if (partition < 0 || partition >= numPartitions) {
            throw new IllegalArgumentException("Partition must be >=0 and < numPartition which is "+numPartitions);
        }
        this.incZeroBasedPartition = partition;
        this.incNumPartitions = numPartitions;
        this.include = null;
        this.exclude = null;
        this.includeValues = null;
        this.excludeValues = null;
        
    }

    
    
    /**
     * Read from a stream.
     */
    public IncludeExclude(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            includeValues = null;
            excludeValues = null;
            incZeroBasedPartition = 0;
            incNumPartitions = 0;
            String includeString = in.readOptionalString();
            include = includeString == null ? null : new RegExp(includeString);
            String excludeString = in.readOptionalString();
            exclude = excludeString == null ? null : new RegExp(excludeString);
            return;
        }
        include = null;
        exclude = null;
        if (in.readBoolean()) {
            int size = in.readVInt();
            includeValues = new TreeSet<>();
            for (int i = 0; i < size; i++) {
                includeValues.add(in.readBytesRef());
            }
        } else {
            includeValues = null;
        }
        if (in.readBoolean()) {
            int size = in.readVInt();
            excludeValues = new TreeSet<>();
            for (int i = 0; i < size; i++) {
                excludeValues.add(in.readBytesRef());
            }
        } else {
            excludeValues = null;
        }
        if (in.getVersion().onOrAfter(Version.V_5_2_0_UNRELEASED)) {
            incNumPartitions = in.readVInt();
            incZeroBasedPartition = in.readVInt();
        } else {
            incNumPartitions = 0;
            incZeroBasedPartition = 0;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        boolean regexBased = isRegexBased();
        out.writeBoolean(regexBased);
        if (regexBased) {
            out.writeOptionalString(include == null ? null : include.getOriginalString());
            out.writeOptionalString(exclude == null ? null : exclude.getOriginalString());
        } else {
            boolean hasIncludes = includeValues != null;
            out.writeBoolean(hasIncludes);
            if (hasIncludes) {
                out.writeVInt(includeValues.size());
                for (BytesRef value : includeValues) {
                    out.writeBytesRef(value);
                }
            }
            boolean hasExcludes = excludeValues != null;
            out.writeBoolean(hasExcludes);
            if (hasExcludes) {
                out.writeVInt(excludeValues.size());
                for (BytesRef value : excludeValues) {
                    out.writeBytesRef(value);
                }
            }
            if (out.getVersion().onOrAfter(Version.V_5_2_0_UNRELEASED)) {
                out.writeVInt(incNumPartitions);
                out.writeVInt(incZeroBasedPartition);
            }
        }
    }

    private static SortedSet<BytesRef> convertToBytesRefSet(String[] values) {
        SortedSet<BytesRef> returnSet = null;
        if (values != null) {
            returnSet = new TreeSet<>();
            for (String value : values) {
                returnSet.add(new BytesRef(value));
            }
        }
        return returnSet;
    }

    private static SortedSet<BytesRef> convertToBytesRefSet(double[] values) {
        SortedSet<BytesRef> returnSet = null;
        if (values != null) {
            returnSet = new TreeSet<>();
            for (double value : values) {
                returnSet.add(new BytesRef(String.valueOf(value)));
            }
        }
        return returnSet;
    }

    private static SortedSet<BytesRef> convertToBytesRefSet(long[] values) {
        SortedSet<BytesRef> returnSet = null;
        if (values != null) {
            returnSet = new TreeSet<>();
            for (long value : values) {
                returnSet.add(new BytesRef(String.valueOf(value)));
            }
        }
        return returnSet;
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

        public boolean token(String currentFieldName, XContentParser.Token token, XContentParser parser,
                ParseFieldMatcher parseFieldMatcher, Map<ParseField, Object> otherOptions) throws IOException {

            if (token == XContentParser.Token.VALUE_STRING) {
                if (parseFieldMatcher.match(currentFieldName, INCLUDE_FIELD)) {
                    otherOptions.put(INCLUDE_FIELD, parser.text());
                } else if (parseFieldMatcher.match(currentFieldName, EXCLUDE_FIELD)) {
                    otherOptions.put(EXCLUDE_FIELD, parser.text());
                } else {
                    return false;
                }
                return true;
            }

            if (token == XContentParser.Token.START_ARRAY) {
                if (parseFieldMatcher.match(currentFieldName, INCLUDE_FIELD)) {
                    otherOptions.put(INCLUDE_FIELD, new TreeSet<>(parseArrayToSet(parser)));
                     return true;
                }
                if (parseFieldMatcher.match(currentFieldName, EXCLUDE_FIELD)) {
                    otherOptions.put(EXCLUDE_FIELD, new TreeSet<>(parseArrayToSet(parser)));
                      return true;
                }
                return false;
            }

            if (token == XContentParser.Token.START_OBJECT) {
                if (parseFieldMatcher.match(currentFieldName, INCLUDE_FIELD)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        
                        // This "include":{"pattern":"foo.*"} syntax is undocumented since 2.0
                        // Regexes should be "include":"foo.*" 
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            if (parseFieldMatcher.match(currentFieldName, PATTERN_FIELD)) {
                                otherOptions.put(INCLUDE_FIELD, parser.text());
                            } else {
                                throw new ElasticsearchParseException(
                                        "Unknown string parameter in Include/Exclude clause: " + currentFieldName);
                            }
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            if (parseFieldMatcher.match(currentFieldName, NUM_PARTITIONS_FIELD)) {
                                otherOptions.put(NUM_PARTITIONS_FIELD, parser.intValue());
                            } else if (parseFieldMatcher.match(currentFieldName, PARTITION_FIELD)) {
                                otherOptions.put(INCLUDE_FIELD, parser.intValue());
                            } else {
                                throw new ElasticsearchParseException(
                                        "Unknown numeric parameter in Include/Exclude clause: " + currentFieldName);
                            }
                        }
                    }
                } else if (parseFieldMatcher.match(currentFieldName, EXCLUDE_FIELD)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            if (parseFieldMatcher.match(currentFieldName, PATTERN_FIELD)) {
                                otherOptions.put(EXCLUDE_FIELD, parser.text());
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

        public IncludeExclude createIncludeExclude(Map<ParseField, Object> otherOptions) {
            Object includeObject = otherOptions.get(INCLUDE_FIELD);
            String include = null;
            int partition = -1;
            int numPartitions = -1;
            SortedSet<BytesRef> includeValues = null;
            if (includeObject != null) {
                if (includeObject instanceof String) {
                    include = (String) includeObject;
                } else if (includeObject instanceof SortedSet) {
                    includeValues = (SortedSet<BytesRef>) includeObject;
                } else if (includeObject instanceof Integer) {
                    partition = (Integer) includeObject;
                    Object numPartitionsObject = otherOptions.get(NUM_PARTITIONS_FIELD);
                    if (numPartitionsObject instanceof Integer) {
                        numPartitions = (Integer) numPartitionsObject;
                        if (numPartitions < 2) {
                            throw new IllegalArgumentException(NUM_PARTITIONS_FIELD.getPreferredName() + " must be >1");
                        }
                        if (partition < 0 || partition >= numPartitions) {
                            throw new IllegalArgumentException(
                                    PARTITION_FIELD.getPreferredName() + " must be >=0 and <" + numPartitions);
                        }
                    } else {
                        if (numPartitionsObject == null) {
                            throw new IllegalArgumentException(NUM_PARTITIONS_FIELD.getPreferredName() + " parameter is missing");
                        }
                        throw new IllegalArgumentException(NUM_PARTITIONS_FIELD.getPreferredName() + " value must be an integer");
                    }
                }
            }
            Object excludeObject = otherOptions.get(EXCLUDE_FIELD);
            if (numPartitions >0 ){
                if(excludeObject!=null){
                    throw new IllegalArgumentException("Partitioned Include cannot be used in combination with excludes");                    
                }
                return new IncludeExclude(partition, numPartitions);
            }
            
            
            String exclude = null;
            SortedSet<BytesRef> excludeValues = null;
            if (excludeObject != null) {
                if (excludeObject instanceof String) {
                    exclude = (String) excludeObject;
                } else if (excludeObject instanceof SortedSet) {
                    excludeValues = (SortedSet<BytesRef>) excludeObject;
                }
            }
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

    public boolean isPartitionBased() {
        return incNumPartitions > 0;
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

    public StringFilter convertToStringFilter(DocValueFormat format) {
        if (isRegexBased()) {
            return new AutomatonBackedStringFilter(toAutomaton());
        }
        if (isPartitionBased()){
            return new PartitionedStringFilter();
        }
        return new TermListBackedStringFilter(parseForDocValues(includeValues, format), parseForDocValues(excludeValues, format));
    }

    private static SortedSet<BytesRef> parseForDocValues(SortedSet<BytesRef> endUserFormattedValues, DocValueFormat format) {
        SortedSet<BytesRef> result = endUserFormattedValues;
        if (endUserFormattedValues != null) {
            if (format != DocValueFormat.RAW) {
                result = new TreeSet<>();
                for (BytesRef formattedVal : endUserFormattedValues) {
                    result.add(format.parseBytesRef(formattedVal.utf8ToString()));
                }
            }
        }
        return result;
    }

    public OrdinalsFilter convertToOrdinalsFilter(DocValueFormat format) {

        if (isRegexBased()) {
            return new AutomatonBackedOrdinalsFilter(toAutomaton());
        }
        if (isPartitionBased()){
            return new PartitionedOrdinalsFilter();
        }
        
        return new TermListBackedOrdinalsFilter(parseForDocValues(includeValues, format), parseForDocValues(excludeValues, format));
    }

    public LongFilter convertToLongFilter(DocValueFormat format) {
        
        if(isPartitionBased()){
            return new PartitionedLongFilter();
        }
        
        int numValids = includeValues == null ? 0 : includeValues.size();
        int numInvalids = excludeValues == null ? 0 : excludeValues.size();
        SetBackedLongFilter result = new SetBackedLongFilter(numValids, numInvalids);
        if (includeValues != null) {
            for (BytesRef val : includeValues) {
                result.addAccept(format.parseLong(val.utf8ToString(), false, null));
            }
        }
        if (excludeValues != null) {
            for (BytesRef val : excludeValues) {
                result.addReject(format.parseLong(val.utf8ToString(), false, null));
            }
        }
        return result;
    }

    public LongFilter convertToDoubleFilter() {
        if(isPartitionBased()){
            return new PartitionedLongFilter();
        }
        
        int numValids = includeValues == null ? 0 : includeValues.size();
        int numInvalids = excludeValues == null ? 0 : excludeValues.size();
        SetBackedLongFilter result = new SetBackedLongFilter(numValids, numInvalids);
        if (includeValues != null) {
            for (BytesRef val : includeValues) {
                double dval = Double.parseDouble(val.utf8ToString());
                result.addAccept(NumericUtils.doubleToSortableLong(dval));
            }
        }
        if (excludeValues != null) {
            for (BytesRef val : excludeValues) {
                double dval = Double.parseDouble(val.utf8ToString());
                result.addReject(NumericUtils.doubleToSortableLong(dval));
            }
        }
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (include != null) {
            builder.field(INCLUDE_FIELD.getPreferredName(), include.getOriginalString());
        }
        if (includeValues != null) {
            builder.startArray(INCLUDE_FIELD.getPreferredName());
            for (BytesRef value : includeValues) {
                builder.value(value.utf8ToString());
            }
            builder.endArray();
        }
        if (exclude != null) {
            builder.field(EXCLUDE_FIELD.getPreferredName(), exclude.getOriginalString());
        }
        if (excludeValues != null) {
            builder.startArray(EXCLUDE_FIELD.getPreferredName());
            for (BytesRef value : excludeValues) {
                builder.value(value.utf8ToString());
            }
            builder.endArray();
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(include == null ? null : include.getOriginalString(), exclude == null ? null : exclude.getOriginalString(),
                includeValues, excludeValues);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        } if (getClass() != obj.getClass()) {
            return false;
        }
        IncludeExclude other = (IncludeExclude) obj;
        return Objects.equals(include == null ? null : include.getOriginalString(), other.include == null ? null : other.include.getOriginalString())
                && Objects.equals(exclude == null ? null : exclude.getOriginalString(), other.exclude == null ? null : other.exclude.getOriginalString())
                && Objects.equals(includeValues, other.includeValues)
                && Objects.equals(excludeValues, other.excludeValues);
    }

}
