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
import java.util.Set;
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
 * <p>The request {@code size} is the EQL engine's default unless a {@code WITH {"size"}} option sets it. A row
 * {@code LIMIT} folded into the plan ({@code pushedLimit}) can only lower it, never raise it.
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
    private record Option(Class<?> type, String typeName, int min, Set<String> allowedValues, BiConsumer<EqlSearchRequest, Object> apply) {}

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
        // result_position is the one string option with a closed value set; validate it at parse time (the others are
        // free-form field names) so a miscased "TAIL" fails loud here rather than as a late engine-worded error.
        stringOption(options, "result_position", Set.of("head", "tail"), EqlSearchRequest::resultPosition);
        // The one EQL knob ESQL has no equivalent for: whether a sequence that spanned a failed shard may be
        // returned. Defaulted false in build() (fail-safe) and opted into here.
        options.put(
            "allow_partial_sequence_results",
            new Option(Boolean.class, "boolean", 0, Set.of(), (request, value) -> request.allowPartialSequenceResults((Boolean) value))
        );
        return options;
    }

    // Numeric options arrive as folded Number literals; the request takes an int. min is the inclusive lower bound.
    private static void intOption(Map<String, Option> options, String name, int min, ObjIntConsumer<EqlSearchRequest> apply) {
        options.put(
            name,
            new Option(Number.class, "numeric", min, Set.of(), (request, value) -> apply.accept(request, ((Number) value).intValue()))
        );
    }

    private static void stringOption(Map<String, Option> options, String name, BiConsumer<EqlSearchRequest, String> apply) {
        stringOption(options, name, Set.of(), apply);
    }

    // A string option, optionally restricted to a closed set of allowed values (empty set = any string).
    private static void stringOption(
        Map<String, Option> options,
        String name,
        Set<String> allowedValues,
        BiConsumer<EqlSearchRequest, String> apply
    ) {
        options.put(name, new Option(String.class, "string", 0, allowedValues, (request, value) -> apply.accept(request, (String) value)));
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
            // A string option with a closed value set (result_position) must be one of them — reject a mistyped value
            // here rather than letting it surface as a late engine-worded error at execution.
            if (option.allowedValues().isEmpty() == false && option.allowedValues().contains(entry.getValue()) == false) {
                throw new ParsingException(
                    source,
                    "EQL command option ["
                        + entry.getKey()
                        + "] value ["
                        + entry.getValue()
                        + "] must be one of "
                        + option.allowedValues().stream().sorted().toList()
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
     * same query: the result-truncation cap (the {@code size} fallback), the partial-results contract, cross-project
     * routing, and the out-of-band request {@code filter}.
     */
    public record EnclosingQuery(boolean allowPartialSearchResults, @Nullable String projectRouting, @Nullable QueryBuilder filter) {}

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
        applyOptions(request, options);
        if (pushedLimit != null) {
            request.size(Math.min(pushedLimit, request.size()));
        }
        return request;
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
