/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongBitSet;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.hppc.BitMixer;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.RegExp;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Defines the include/exclude regular expression filtering for string terms aggregation. In this filtering logic,
 * exclusion has precedence, where the {@code include} is evaluated first and then the {@code exclude}.
 */
public class IncludeExclude implements Writeable, ToXContentFragment {
    public static final ParseField INCLUDE_FIELD = new ParseField("include");
    public static final ParseField EXCLUDE_FIELD = new ParseField("exclude");
    public static final ParseField PARTITION_FIELD = new ParseField("partition");
    public static final ParseField NUM_PARTITIONS_FIELD = new ParseField("num_partitions");
    // Needed to add this seed for a deterministic term hashing policy
    // otherwise tests fail to get expected results and worse, shards
    // can disagree on which terms hash to the required partition.
    private static final int HASH_PARTITIONING_SEED = 31;

    // for parsing purposes only
    // TODO: move all aggs to the same package so that this stuff could be pkg-private
    public static IncludeExclude merge(IncludeExclude include, IncludeExclude exclude) {
        if (include == null) {
            return exclude;
        }
        if (exclude == null) {
            return include;
        }
        if (include.isPartitionBased()) {
            throw new IllegalArgumentException("Cannot specify any excludes when using a partition-based include");
        }

        return new IncludeExclude(
            include.include == null ? null : include.include.getOriginalString(),
            exclude.exclude == null ? null : exclude.exclude.getOriginalString(),
            include.includeValues,
            exclude.excludeValues
        );
    }

    public static IncludeExclude parseInclude(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            return new IncludeExclude(parser.text(), null, null, null);
        } else if (token == XContentParser.Token.START_ARRAY) {
            return new IncludeExclude(null, null, new TreeSet<>(parseArrayToSet(parser)), null);
        } else if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            Integer partition = null, numPartitions = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (NUM_PARTITIONS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    numPartitions = parser.intValue();
                } else if (PARTITION_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    partition = parser.intValue();
                } else {
                    throw new ElasticsearchParseException("Unknown parameter in Include/Exclude clause: " + currentFieldName);
                }
            }
            if (partition == null) {
                throw new IllegalArgumentException(
                    "Missing [" + PARTITION_FIELD.getPreferredName() + "] parameter for partition-based include"
                );
            }
            if (numPartitions == null) {
                throw new IllegalArgumentException(
                    "Missing [" + NUM_PARTITIONS_FIELD.getPreferredName() + "] parameter for partition-based include"
                );
            }
            return new IncludeExclude(partition, numPartitions);
        } else {
            throw new IllegalArgumentException("Unrecognized token for an include [" + token + "]");
        }
    }

    public static IncludeExclude parseExclude(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            return new IncludeExclude(null, parser.text(), null, null);
        } else if (token == XContentParser.Token.START_ARRAY) {
            return new IncludeExclude(null, null, null, new TreeSet<>(parseArrayToSet(parser)));
        } else {
            throw new IllegalArgumentException("Unrecognized token for an exclude [" + token + "]");
        }
    }

    public abstract static class Filter {}

    // The includeValue and excludeValue ByteRefs which are the result of the parsing
    // process are converted into a LongFilter when used on numeric fields
    // in the index.
    public abstract static class LongFilter extends Filter {
        public abstract boolean accept(long value);

    }

    public class PartitionedLongFilter extends LongFilter {
        @Override
        public boolean accept(long value) {
            // hash the value to keep even distributions
            final long hashCode = BitMixer.mix64(value);
            return Math.floorMod(hashCode, incNumPartitions) == incZeroBasedPartition;
        }
    }

    public static class SetBackedLongFilter extends LongFilter {
        // Autoboxing long could cause allocations when doing Set.contains, so
        // this alternative to java.lang.Long is not final so that a preallocated instance
        // can be used in accept (note that none of this is threadsafe!)
        private static class Long {
            private long value;

            private Long(long value) {
                this.value = value;
            }

            @Override
            public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null || getClass() != o.getClass()) return false;
                Long that = (Long) o;
                return value == that.value;
            }

            @Override
            public int hashCode() {
                return java.lang.Long.hashCode(value);
            }
        }

        private Set<Long> valids;
        private Set<Long> invalids;

        private Long spare = new Long(0);

        private SetBackedLongFilter(int numValids, int numInvalids) {
            if (numValids > 0) {
                valids = Sets.newHashSetWithExpectedSize(numValids);
            }
            if (numInvalids > 0) {
                invalids = Sets.newHashSetWithExpectedSize(numInvalids);
            }
        }

        @Override
        public boolean accept(long value) {
            spare.value = value;
            return (valids == null || valids.contains(spare)) && (invalids == null || invalids.contains(spare) == false);
        }

        private void addAccept(long val) {
            valids.add(new Long(val));
        }

        private void addReject(long val) {
            invalids.add(new Long(val));
        }
    }

    // Only used for the 'map' execution mode (ie. scripts)
    public abstract static class StringFilter extends Filter {
        public abstract boolean accept(BytesRef value);
    }

    class PartitionedStringFilter extends StringFilter {
        @Override
        public boolean accept(BytesRef value) {
            return Math.floorMod(StringHelper.murmurhash3_x86_32(value, HASH_PARTITIONING_SEED), incNumPartitions) == incZeroBasedPartition;
        }
    }

    class SetAndRegexStringFilter extends StringFilter {

        private final ByteRunAutomaton runAutomaton;
        private final Set<BytesRef> valids;
        private final Set<BytesRef> invalids;

        private SetAndRegexStringFilter(DocValueFormat format) {
            Automaton automaton = toAutomaton();
            this.runAutomaton = automaton == null ? null : new ByteRunAutomaton(automaton);
            this.valids = parseForDocValues(includeValues, format);
            this.invalids = parseForDocValues(excludeValues, format);
        }

        /**
         * Returns whether the given value is accepted based on the {@code includeValues} &amp; {@code excludeValues}
         * sets, as well as the {@code include} &amp; {@code exclude} patterns.
         */
        @Override
        public boolean accept(BytesRef value) {
            if (valids != null && valids.contains(value) == false) {
                return false;
            }

            if (runAutomaton != null && runAutomaton.run(value.bytes, value.offset, value.length) == false) {
                return false;
            }

            return invalids == null || invalids.contains(value) == false;
        }
    }

    public abstract static class OrdinalsFilter extends Filter {
        public abstract LongBitSet acceptedGlobalOrdinals(SortedSetDocValues globalOrdinals) throws IOException;
    }

    class PartitionedOrdinalsFilter extends OrdinalsFilter {

        @Override
        public LongBitSet acceptedGlobalOrdinals(SortedSetDocValues globalOrdinals) throws IOException {
            final long numOrds = globalOrdinals.getValueCount();
            final LongBitSet acceptedGlobalOrdinals = new LongBitSet(numOrds);
            final TermsEnum termEnum = globalOrdinals.termsEnum();

            BytesRef term = termEnum.next();
            while (term != null) {
                if (Math.floorMod(
                    StringHelper.murmurhash3_x86_32(term, HASH_PARTITIONING_SEED),
                    incNumPartitions
                ) == incZeroBasedPartition) {
                    acceptedGlobalOrdinals.set(termEnum.ord());
                }
                term = termEnum.next();
            }
            return acceptedGlobalOrdinals;
        }
    }

    class SetAndRegexOrdinalsFilter extends OrdinalsFilter {

        private final CompiledAutomaton compiled;
        private final SortedSet<BytesRef> valids;
        private final SortedSet<BytesRef> invalids;

        private SetAndRegexOrdinalsFilter(DocValueFormat format) {
            Automaton automaton = toAutomaton();
            this.compiled = automaton == null ? null : new CompiledAutomaton(automaton);
            this.valids = parseForDocValues(includeValues, format);
            this.invalids = parseForDocValues(excludeValues, format);
        }

        /**
         * Computes which global ordinals are accepted by this IncludeExclude instance, based on the combination of
         * the {@code includeValues} &amp; {@code excludeValues} sets, as well as the {@code include} &amp;
         * {@code exclude} patterns.
         */
        @Override
        public LongBitSet acceptedGlobalOrdinals(SortedSetDocValues globalOrdinals) throws IOException {
            LongBitSet acceptedGlobalOrdinals = null;
            if (valids != null) {
                acceptedGlobalOrdinals = new LongBitSet(globalOrdinals.getValueCount());
                for (BytesRef term : valids) {
                    long ord = globalOrdinals.lookupTerm(term);
                    if (ord >= 0) {
                        acceptedGlobalOrdinals.set(ord);
                    }
                }
            }

            if (compiled != null) {
                LongBitSet automatonGlobalOrdinals = new LongBitSet(globalOrdinals.getValueCount());
                TermsEnum globalTermsEnum;
                Terms globalTerms = new DocValuesTerms(globalOrdinals);
                // TODO: specialize based on compiled.type: for ALL and prefixes (sinkState >= 0 ) we can avoid i/o and just set bits.
                globalTermsEnum = compiled.getTermsEnum(globalTerms);
                for (BytesRef term = globalTermsEnum.next(); term != null; term = globalTermsEnum.next()) {
                    automatonGlobalOrdinals.set(globalTermsEnum.ord());
                }

                if (acceptedGlobalOrdinals == null) {
                    acceptedGlobalOrdinals = automatonGlobalOrdinals;
                } else {
                    acceptedGlobalOrdinals.and(automatonGlobalOrdinals);
                }
            }

            if (acceptedGlobalOrdinals == null) {
                acceptedGlobalOrdinals = new LongBitSet(globalOrdinals.getValueCount());
                if (acceptedGlobalOrdinals.length() > 0) {
                    // default to all terms being acceptable
                    acceptedGlobalOrdinals.set(0, acceptedGlobalOrdinals.length());
                }
            }

            if (invalids != null) {
                for (BytesRef term : invalids) {
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
    public IncludeExclude(
        @Nullable String include,
        @Nullable String exclude,
        @Nullable SortedSet<BytesRef> includeValues,
        @Nullable SortedSet<BytesRef> excludeValues
    ) {
        if (include == null && exclude == null && includeValues == null && excludeValues == null) {
            throw new IllegalArgumentException();
        }
        if (include != null && includeValues != null) {
            throw new IllegalArgumentException();
        }
        if (exclude != null && excludeValues != null) {
            throw new IllegalArgumentException();
        }
        this.include = include == null ? null : new RegExp(include);
        this.exclude = exclude == null ? null : new RegExp(exclude);
        this.includeValues = includeValues;
        this.excludeValues = excludeValues;
        this.incZeroBasedPartition = 0;
        this.incNumPartitions = 0;
    }

    public IncludeExclude(int partition, int numPartitions) {
        if (partition < 0 || partition >= numPartitions) {
            throw new IllegalArgumentException("Partition must be >=0 and < numPartition which is " + numPartitions);
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
            String includeString = in.readOptionalString();
            include = includeString == null ? null : new RegExp(includeString);
            String excludeString = in.readOptionalString();
            exclude = excludeString == null ? null : new RegExp(excludeString);
            if (in.getVersion().before(Version.V_7_11_0)) {
                incZeroBasedPartition = 0;
                incNumPartitions = 0;
                includeValues = null;
                excludeValues = null;
                return;
            }
        } else {
            include = null;
            exclude = null;
        }
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
        incNumPartitions = in.readVInt();
        incZeroBasedPartition = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        boolean regexBased = isRegexBased();
        out.writeBoolean(regexBased);
        if (regexBased) {
            out.writeOptionalString(include == null ? null : include.getOriginalString());
            out.writeOptionalString(exclude == null ? null : exclude.getOriginalString());
            if (out.getVersion().before(Version.V_7_11_0)) {
                return;
            }
        }
        boolean hasIncludes = includeValues != null;
        out.writeBoolean(hasIncludes);
        if (hasIncludes) {
            out.writeCollection(includeValues, StreamOutput::writeBytesRef);
        }
        boolean hasExcludes = excludeValues != null;
        out.writeBoolean(hasExcludes);
        if (hasExcludes) {
            out.writeCollection(excludeValues, StreamOutput::writeBytesRef);
        }
        out.writeVInt(incNumPartitions);
        out.writeVInt(incZeroBasedPartition);
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

    private static Set<BytesRef> parseArrayToSet(XContentParser parser) throws IOException {
        final Set<BytesRef> set = new HashSet<>();
        if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
            throw new ElasticsearchParseException("Missing start of array in include/exclude clause");
        }
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (parser.currentToken().isValue() == false) {
                throw new ElasticsearchParseException("Array elements in include/exclude clauses should be string values");
            }
            set.add(new BytesRef(parser.text()));
        }
        return set;
    }

    public boolean isRegexBased() {
        return include != null || exclude != null;
    }

    public boolean isPartitionBased() {
        return incNumPartitions > 0;
    }

    private Automaton toAutomaton() {
        Automaton a = null;
        if (include == null && exclude == null) {
            return a;
        }
        if (include != null) {
            a = include.toAutomaton();
        } else {
            a = Automata.makeAnyString();
        }
        if (exclude != null) {
            a = Operations.minus(a, exclude.toAutomaton(), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        }
        return a;
    }

    public StringFilter convertToStringFilter(DocValueFormat format) {
        if (isPartitionBased()) {
            return new PartitionedStringFilter();
        }
        return new SetAndRegexStringFilter(format);
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
        if (isPartitionBased()) {
            return new PartitionedOrdinalsFilter();
        }

        return new SetAndRegexOrdinalsFilter(format);
    }

    public LongFilter convertToLongFilter(DocValueFormat format) {

        if (isPartitionBased()) {
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
        if (isPartitionBased()) {
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
        } else if (includeValues != null) {
            builder.startArray(INCLUDE_FIELD.getPreferredName());
            for (BytesRef value : includeValues) {
                builder.value(value.utf8ToString());
            }
            builder.endArray();
        } else if (isPartitionBased()) {
            builder.startObject(INCLUDE_FIELD.getPreferredName());
            builder.field(PARTITION_FIELD.getPreferredName(), incZeroBasedPartition);
            builder.field(NUM_PARTITIONS_FIELD.getPreferredName(), incNumPartitions);
            builder.endObject();
        }
        if (exclude != null) {
            builder.field(EXCLUDE_FIELD.getPreferredName(), exclude.getOriginalString());
        } else if (excludeValues != null) {
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
        return Objects.hash(
            include == null ? null : include.getOriginalString(),
            exclude == null ? null : exclude.getOriginalString(),
            includeValues,
            excludeValues,
            incZeroBasedPartition,
            incNumPartitions
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        IncludeExclude other = (IncludeExclude) obj;
        return Objects.equals(
            include == null ? null : include.getOriginalString(),
            other.include == null ? null : other.include.getOriginalString()
        )
            && Objects.equals(
                exclude == null ? null : exclude.getOriginalString(),
                other.exclude == null ? null : other.exclude.getOriginalString()
            )
            && Objects.equals(includeValues, other.includeValues)
            && Objects.equals(excludeValues, other.excludeValues)
            && Objects.equals(incZeroBasedPartition, other.incZeroBasedPartition)
            && Objects.equals(incNumPartitions, other.incNumPartitions);
    }

}
