/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.eql;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.session.IndexResolver;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.ObjIntConsumer;

import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;

/**
 * Builds an {@link EqlSearchRequest} from the {@code EQL <indexPattern> "<query>"} command's index pattern,
 * query string, resolved schema and {@code WITH { ... }} tuning options. Kept small and pure so it is
 * unit-testable without a client.
 *
 * <p>The schema drives the fields the EQL engine fetches per event: one {@link FieldAndFormat} per mapped,
 * convertible {@link FieldAttribute} (synthetics, metadata and unsupported columns carry no wire field), with the
 * {@code epoch_millis} format on date columns so the converter reads a stable epoch value. Unmapped columns added
 * under {@code SET unmapped_fields} follow their kind: nullified ({@code NULL}) columns carry no fetch, LOAD columns
 * fetch from {@code _source} with {@code include_unmapped}.
 *
 * <p>The request {@code size} follows an explicit precedence: a {@code WITH {"size"}} option wins; otherwise the
 * row {@code LIMIT} folded into the plan ({@code pushedLimit}); otherwise the ES|QL result-truncation cap
 * ({@code defaultSize}). Only the last case can silently truncate, so the caller warns on it (see
 * {@link #usesTruncationCapSize}).
 *
 * <p>The supported {@code WITH} options are the single source of truth in {@link #OPTIONS}: {@link #validateOptions}
 * rejects anything else (or a wrong-typed value) at parse time, and {@link #applyOptions} applies exactly these.
 */
public final class EqlRequests {

    private EqlRequests() {}

    /**
     * A supported {@code WITH} option: the value type it requires, a human name for that type used in error
     * messages, and how the value applies to the request. {@link #OPTIONS} is the single source of truth for the
     * command's option surface, so validation and application never drift and adding an option is one entry.
     */
    private record Option(Class<?> type, String typeName, int min, BiConsumer<EqlSearchRequest, Object> apply) {}

    private static final Map<String, Option> OPTIONS = options();

    private static Map<String, Option> options() {
        Map<String, Option> options = new LinkedHashMap<>();
        // Minimums mirror EqlSearchRequest.validate() so a below-range value fails at parse, not mid-execution.
        intOption(options, "size", 0, EqlSearchRequest::size);
        intOption(options, "fetch_size", 2, EqlSearchRequest::fetchSize);
        intOption(options, "max_samples_per_key", 1, EqlSearchRequest::maxSamplesPerKey);
        stringOption(options, "timestamp_field", EqlSearchRequest::timestampField);
        stringOption(options, "tiebreaker_field", EqlSearchRequest::tiebreakerField);
        stringOption(options, "event_category_field", EqlSearchRequest::eventCategoryField);
        stringOption(options, "result_position", EqlSearchRequest::resultPosition);
        // The one EQL knob ESQL has no equivalent for: whether a sequence that spanned a failed shard may be
        // returned. Defaulted false in build() (fail-safe) and opted into here.
        options.put(
            "allow_partial_sequence_results",
            new Option(Boolean.class, "boolean", 0, (request, value) -> request.allowPartialSequenceResults((Boolean) value))
        );
        return options;
    }

    // Numeric options arrive as folded Number literals; the request takes an int. min is the inclusive lower bound.
    private static void intOption(Map<String, Option> options, String name, int min, ObjIntConsumer<EqlSearchRequest> apply) {
        options.put(name, new Option(Number.class, "numeric", min, (request, value) -> apply.accept(request, ((Number) value).intValue())));
    }

    private static void stringOption(Map<String, Option> options, String name, BiConsumer<EqlSearchRequest, String> apply) {
        options.put(name, new Option(String.class, "string", 0, (request, value) -> apply.accept(request, (String) value)));
    }

    /**
     * Rejects any {@code WITH} option the command does not support, or one supplied with the wrong value type.
     * Runs at parse time — alongside the {@code indices} rejection in the parser — so a typo (silently querying a
     * default field) or a mistyped value (silently ignored) fails fast and loud instead of returning wrong results
     * as complete.
     */
    public static void validateOptions(Source source, Map<String, Object> options) {
        for (Map.Entry<String, Object> entry : options.entrySet()) {
            Option option = OPTIONS.get(entry.getKey());
            if (option == null) {
                throw new ParsingException(
                    source,
                    "unknown EQL command option [" + entry.getKey() + "], expected one of " + OPTIONS.keySet()
                );
            }
            if (option.type().isInstance(entry.getValue()) == false) {
                throw new ParsingException(
                    source,
                    "EQL command option [" + entry.getKey() + "] requires a " + option.typeName() + " value"
                );
            }
            // Every numeric option applies as an int within [min, Integer.MAX_VALUE]. Reject a value that is
            // fractional, below its minimum, or outside int range rather than letting intValue() silently truncate
            // or wrap it: size 4294967296 and -4294967296 both collapse to 0 (an empty result presented as
            // complete), size 3.9 would become 3, and fetch_size 1 would be rejected only later by the delegate.
            if (entry.getValue() instanceof Number number) {
                long asLong = number.longValue();
                if (number.doubleValue() != asLong || asLong < option.min() || asLong > Integer.MAX_VALUE) {
                    throw new ParsingException(
                        source,
                        "EQL command option ["
                            + entry.getKey()
                            + "] value ["
                            + entry.getValue()
                            + "] must be an integer between "
                            + option.min()
                            + " and "
                            + Integer.MAX_VALUE
                    );
                }
            }
        }
    }

    /**
     * The settings an EQL source inherits from the ES|QL query that hosts it — bridged from the query, not from the
     * command's own {@code WITH} options, so an EQL source honors the same contract as a {@code FROM} source in the
     * same query: the result-truncation cap (the {@code size} fallback), the partial-results contract, and cross-project
     * routing. The out-of-band request {@code filter} is reserved for a future bridge and is always null today —
     * {@code EsqlSession} rejects a query that combines a request filter with an EQL source rather than applying it
     * post-hoc, which would strip events out of sequence matches.
     */
    public record EnclosingQuery(
        int truncationCap,
        boolean allowPartialSearchResults,
        @Nullable String projectRouting,
        @Nullable QueryBuilder filter
    ) {}

    public static EqlSearchRequest build(
        String query,
        String indices,
        List<Attribute> schema,
        Map<String, Object> options,
        Integer pushedLimit,
        EnclosingQuery enclosing
    ) {
        if (indices == null || indices.isBlank()) {
            throw new EsqlIllegalArgumentException("EQL command requires a non-empty index pattern");
        }
        EqlSearchRequest request = new EqlSearchRequest();
        // Split as RestEqlSearchAction and ES|QL's own field-caps request (IndexResolver) do, so schema resolution
        // and the delegated search see the identical index set. The leading blank-pattern guard above rejects empties.
        request.indices(Strings.splitStringByCommaToArray(indices));
        // Resolve and execute over the same index set: ES|QL resolved the schema under IndexResolver.DEFAULT_OPTIONS, so
        // pin the same options here (the command surface differs from standalone _eql/search defaults). This is also the
        // prerequisite that makes reusing the resolved field-caps sound.
        request.indicesOptions(IndexResolver.DEFAULT_OPTIONS);
        request.query(query);
        // Honor the enclosing ES|QL query's own partial-results contract: an event source then behaves exactly like a
        // FROM source under a shard failure. Sequences are a separate axis, defaulted fail-safe just below.
        request.allowPartialSearchResults(enclosing.allowPartialSearchResults());
        // Fail-safe default: a sequence that lost a stage on a failed shard is a corrupt match, not a shorter one.
        // A WITH {"allow_partial_sequence_results": true} option opts into resilience over completeness.
        request.allowPartialSequenceResults(false);
        if (enclosing.projectRouting() != null) {
            request.projectRouting(enclosing.projectRouting());
        }
        if (enclosing.filter() != null) {
            request.filter(enclosing.filter());
        }
        List<FieldAndFormat> fetchFields = fetchFields(schema);
        if (fetchFields.isEmpty() == false) {
            request.fetchFields(fetchFields);
        }
        // ES|QL LIMIT selects the first n rows, but the EQL engine defaults result_position to "tail" (the last n).
        // Under "tail" a pushed size fetches a suffix while the retained LIMIT trims a prefix, so a downstream op that
        // blocks the push (e.g. WHERE) would flip which rows come back. Default to "head" so a pushed size and a
        // cap-fetch both take the prefix and LIMIT is stable w.r.t. the pushdown. A WITH {"result_position"} still
        // overrides this in applyOptions.
        request.resultPosition("head");
        // Effective size default; a WITH {"size"} option overrides it in applyOptions.
        request.size(pushedLimit != null ? pushedLimit : enclosing.truncationCap());
        applyOptions(request, options);
        return request;
    }

    /**
     * Whether the effective request size came from the truncation cap (no {@code WITH {"size"}} and no pushed
     * {@code LIMIT}) — the only case where a full response may be silently incomplete, so the caller warns on it.
     */
    public static boolean usesTruncationCapSize(Map<String, Object> options, Integer pushedLimit) {
        return options.get("size") instanceof Number == false && pushedLimit == null;
    }

    /**
     * One fetch entry per mapped field column. Synthetics ({@code ReferenceAttribute}), metadata columns
     * ({@code MetadataAttribute} — their values come from the response envelope, not the fields API) and unsupported
     * columns are skipped: only real {@code FieldAttribute}s (excluding the {@code UnsupportedAttribute} subtype) fetch.
     * Unmapped columns added under {@code SET unmapped_fields}: a {@code NULL}-typed (nullified) column carries no fetch
     * entry, while a {@code LOAD}-mode column ({@code PotentiallyUnmappedKeywordEsField}) fetches from {@code _source}
     * with {@code include_unmapped}.
     */
    private static List<FieldAndFormat> fetchFields(List<Attribute> schema) {
        List<FieldAndFormat> fields = new ArrayList<>();
        for (Attribute attribute : schema) {
            // UnsupportedAttribute extends FieldAttribute, so exclude it explicitly — it has no extractable wire value.
            if (attribute instanceof FieldAttribute fa && attribute instanceof UnsupportedAttribute == false) {
                // A nullified unmapped column (SET unmapped_fields=nullify) is NULL-typed and produces no value —
                // the converter constant-nulls it, so it carries no fetch entry.
                if (fa.dataType() == NULL) {
                    continue;
                }
                String format = fa.dataType() == DATETIME ? "epoch_millis" : null;
                // A LOAD-mode unmapped column is backed by PotentiallyUnmappedKeywordEsField; fetch it from _source
                // with include_unmapped=true, the same way FROM loads it.
                if (fa.field() instanceof PotentiallyUnmappedKeywordEsField) {
                    fields.add(new FieldAndFormat(fa.fieldName().string(), format, true));
                } else {
                    fields.add(new FieldAndFormat(fa.fieldName().string(), format));
                }
            }
        }
        return fields;
    }

    /** Applies each supported option present in {@code options}; unknown keys were already rejected by parse-time validation. */
    private static void applyOptions(EqlSearchRequest request, Map<String, Object> options) {
        for (Map.Entry<String, Option> supported : OPTIONS.entrySet()) {
            Object value = options.get(supported.getKey());
            if (value != null) {
                supported.getValue().apply().accept(request, value);
            }
        }
    }
}
