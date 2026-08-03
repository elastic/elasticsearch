/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.derivedmetrics;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.mapper.DataStreamTimestampFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.TextFieldMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads the values derived metrics needs out of the document Elasticsearch has <em>already parsed</em>, rather than parsing
 * {@code _source} a second time.
 *
 * <p>By the time an indexing listener runs, {@code DocumentParser} has turned the document into {@link LuceneDocument}s full of
 * materialised fields. Re-parsing the source to recover values that are sitting right there costs 913ns and 1,848 bytes per document —
 * measured, and 84% of the time and effectively all of the allocation on the write path. Reading the parsed document instead costs a
 * single scan of its fields and allocates nothing.
 *
 * <p>The catch is that not every mapping stores a value the same way it appeared in the source, and a metrics feature that silently
 * changes a dimension value depending on which reader ran would be worse than a slow one. So a path is only read from the document when
 * it is <em>provably identical</em> to what a source parse would have produced; anything else falls back. See
 * {@link #resolve(MappingLookup, List)} for what qualifies and why.
 *
 * <p>This is the contained form of the optimisation. The better one is to have the mappers hand their values over during parse, the way
 * {@code TimeSeriesIdFieldMapper} already collects dimensions through {@code RoutingPathFields}; that removes this scan as well as the
 * parse. It is described in the design note and deliberately not attempted here, because it would couple core mappers to this module.
 */
public final class DerivedMetricsDocumentReader {

    private DerivedMetricsDocumentReader() {}

    /**
     * How one configured path can be recovered from the parsed document.
     */
    public enum Strategy {
        /**
         * A {@code text} field. {@code TextFieldMapper} stores the raw string straight from the parser and leaves analysis to the index
         * writer, so what comes back is exactly the source value. This is what makes the default dynamic mapping — {@code text} with a
         * {@code .keyword} sub-field — readable without touching the sub-field, and therefore without its {@code ignore_above}.
         */
        TEXT,
        /** A keyword field with no normalizer, whose term is the source value verbatim. */
        KEYWORD,
        /** An integral number, stored unencoded. */
        LONG,
        /** A double, stored through {@link NumericUtils#doubleToSortableLong}. */
        DOUBLE,
        /** A float, stored through {@link NumericUtils#floatToSortableInt} and widened. */
        FLOAT,
        /** Nothing about this path can be trusted to match the source; the whole document falls back to a source parse. */
        UNSUPPORTED
    }

    /**
     * The per-path decision for one mapping, worked out once rather than per document.
     *
     * @param bySlot   what to do with each configured path, indexed by its slot
     * @param complete whether every path can be served, which is the only case where the document may be read instead of its source
     */
    public record Strategies(Strategy[] bySlot, boolean complete, Map<String, Integer> slotsByField, boolean timestampInNanos) {}

    /**
     * Decides, for one index mapping, which configured paths can be read back from a parsed document.
     *
     * <p>A path qualifies only when the stored value is the source value. The cases that disqualify it are worth naming, because each is
     * a silent difference rather than a missing value:
     *
     * <ul>
     *   <li><b>A normalizer.</b> The keyword mapper normalises before building the term, so a {@code lowercase} normalizer would turn
     *       {@code PROD-EU} into {@code prod-eu} and quietly split or merge series. Rare, and always deliberate, but never safe.</li>
     *   <li><b>{@code ignore_above}.</b> An over-long value is not stored at all, and absent-because-too-long cannot be told apart from
     *       absent-because-missing — so the dimension would come back null where a source parse produced a value.</li>
     *   <li><b>Anything unmapped</b>, or mapped as a type whose stored form is not recoverable.</li>
     * </ul>
     *
     * <p>Numeric fields are recoverable but not naively: {@code DoubleField} stores
     * {@link NumericUtils#doubleToSortableLong}, so reading {@code numericValue()} without decoding would produce a plausible and
     * completely wrong number. {@code half_float} is excluded outright — it is lossy against the source value, and a metric that
     * silently changes what it sums is exactly what this whole check exists to prevent.
     */
    public static Strategies resolve(MappingLookup mappings, List<String> paths) {
        Strategy[] bySlot = new Strategy[paths.size()];
        Map<String, Integer> slotsByField = new HashMap<>(paths.size());
        boolean complete = true;
        for (int slot = 0; slot < paths.size(); slot++) {
            String path = paths.get(slot);
            Strategy strategy = strategyFor(mappings, path);
            bySlot[slot] = strategy;
            if (strategy == Strategy.UNSUPPORTED) {
                complete = false;
            } else {
                slotsByField.put(path, slot);
            }
        }
        return new Strategies(bySlot, complete, Map.copyOf(slotsByField), timestampInNanos(mappings));
    }

    /**
     * Whether this mapping stores {@code @timestamp} in nanoseconds rather than milliseconds.
     *
     * <p>Resolved once per mapping rather than per document, because it is a property of the mapping and the answer is needed on the
     * write path. A {@code date_nanos} timestamp is stored as nanoseconds since the epoch, so bucketing an observation by it without
     * converting would put the document a million times too far into the future.
     */
    private static boolean timestampInNanos(MappingLookup mappings) {
        Mapper timestamp = mappings.getMapper(DataStreamTimestampFieldMapper.DEFAULT_PATH);
        return timestamp != null && DateFieldMapper.DATE_NANOS_CONTENT_TYPE.equals(timestamp.typeName());
    }

    /**
     * The document's {@code @timestamp} in milliseconds, or {@link #NO_TIMESTAMP} when it cannot be read.
     *
     * <p>It is fetched by key rather than found among the document's fields. The date mapper stores it in a keyed slot precisely so that
     * {@code TsidExtractingIdFieldMapper} does not have to scan, and because it is added with {@code onlyAddKey} it is invisible to the
     * field walk in {@link #read}.
     *
     * <p>It can genuinely be absent. Some failure paths hand over no parsed document at all, and the columnar parse does not record the
     * timestamp yet, so the caller has to have an answer for that rather than assume a data stream guarantees one.
     */
    public static long timestampMillis(ParsedDocument document, Strategies strategies) {
        if (document == null) {
            return NO_TIMESTAMP;
        }
        LuceneDocument root = document.rootDoc();
        if (root == null) {
            return NO_TIMESTAMP;
        }
        IndexableField field = root.getByKey(DataStreamTimestampFieldMapper.TIMESTAMP_VALUE_KEY);
        if (field == null || field.numericValue() == null) {
            return NO_TIMESTAMP;
        }
        long value = field.numericValue().longValue();
        return strategies != null && strategies.timestampInNanos() ? value / 1_000_000L : value;
    }

    /** Returned when a document carries no timestamp this can read. */
    public static final long NO_TIMESTAMP = Long.MIN_VALUE;

    private static Strategy strategyFor(MappingLookup mappings, String path) {
        MappedFieldType type = mappings.getFieldType(path);
        if (type == null) {
            return Strategy.UNSUPPORTED;
        }
        if (type instanceof KeywordFieldMapper.KeywordFieldType keyword) {
            if (keyword.hasNormalizer()) {
                return Strategy.UNSUPPORTED;
            }
            // any effective ignore_above means an over-long value is simply absent, which reads as a missing dimension
            if (keyword.ignoreAbove().valuesPotentiallyIgnored()) {
                return Strategy.UNSUPPORTED;
            }
            // Being mapped is not the same as being present. A keyword that is not indexed puts either nothing in the document at all,
            // or — with high cardinality doc values — an envelope of every value under the same name, which is not the term. Requiring
            // the inverted index is what guarantees the plain term this reader knows how to decode.
            return keyword.isSearchable() ? Strategy.KEYWORD : Strategy.UNSUPPORTED;
        }
        if (type instanceof TextFieldMapper.TextFieldType text) {
            // a text field materialises its raw string only when indexed or stored
            return text.isSearchable() || text.isStored() ? Strategy.TEXT : Strategy.UNSUPPORTED;
        }
        if (type instanceof NumberFieldMapper.NumberFieldType number) {
            // Doc values carry the number itself. Without them an indexed numeric is a points field, whose contents are encoded bytes
            // rather than a value, and reading it would silently produce nothing where the source had something.
            if (number.hasDocValues() == false) {
                return Strategy.UNSUPPORTED;
            }
            return switch (number.numberType()) {
                case LONG, INTEGER, SHORT, BYTE -> Strategy.LONG;
                case DOUBLE -> Strategy.DOUBLE;
                case FLOAT -> Strategy.FLOAT;
                // half_float is lossy against the source value, so reading it would change what a metric sums
                case HALF_FLOAT -> Strategy.UNSUPPORTED;
            };
        }
        return Strategy.UNSUPPORTED;
    }

    /**
     * Fills {@code values} from the already-parsed document, in one pass over its root fields.
     *
     * <p>Fields are matched by name against the configured paths rather than looked up one path at a time, because
     * {@link LuceneDocument#getFields(String)} scans every field in the document and allocates a list each time it is called — five
     * configured paths on a wide log document would be five scans and five lists.
     *
     * @return whether every configured path was read. False means the caller must fall back to reading {@code _source}.
     */
    public static boolean read(ParsedDocument document, Strategies strategies, Object[] values) {
        if (strategies.complete() == false || document == null) {
            return false;
        }
        LuceneDocument root = document.rootDoc();
        if (root == null) {
            return false;
        }
        for (IndexableField field : root.getFields()) {
            Integer slot = strategies.slotsByField().get(field.name());
            if (slot == null) {
                continue;
            }
            Object value = valueOf(field, strategies.bySlot()[slot]);
            if (value == null) {
                continue;
            }
            if (values[slot] != null) {
                // A second field under the same name is a multi-valued field. The source reader hands those back as a list, which every
                // consumer then treats as absent, so the two readers agree by refusing to guess which value was meant.
                values[slot] = null;
                return false;
            }
            values[slot] = value;
        }
        return true;
    }

    /**
     * Decodes one field, or returns null when this particular field is not the one carrying the value.
     *
     * <p>The type check is not a formality. A keyword field with high-cardinality binary doc values and no inverted index puts a
     * length-prefixed envelope of every value under the same name, and reading its bytes as a term would return something that looks like
     * a value and is not one. Only the field classes whose contents are known are accepted.
     */
    private static Object valueOf(IndexableField field, Strategy strategy) {
        return switch (strategy) {
            case TEXT -> field.stringValue();
            case KEYWORD -> field instanceof KeywordFieldMapper.KeywordField && field.binaryValue() != null
                ? field.binaryValue().utf8ToString()
                : null;
            case LONG -> field.numericValue();
            case DOUBLE -> field.numericValue() == null ? null : NumericUtils.sortableLongToDouble(field.numericValue().longValue());
            case FLOAT -> field.numericValue() == null ? null : NumericUtils.sortableIntToFloat((int) field.numericValue().longValue());
            case UNSUPPORTED -> null;
        };
    }
}
