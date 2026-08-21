/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.eql;

import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.fieldAttribute;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link EqlRequests#build} — mapping the {@code EQL <indexPattern> "..." WITH { ... }} command's
 * index pattern, resolved schema and tuning options to an {@link EqlSearchRequest}.
 */
public class EqlRequestsTests extends ESTestCase {

    private static final List<Attribute> NO_SCHEMA = List.of();
    private static final int CAP = 10_000;
    private static final EqlRequests.EnclosingQuery DEFAULT_ENCLOSING = new EqlRequests.EnclosingQuery(CAP, false, null, null);

    /** Delegates to {@link EqlRequests#build} with no pushed limit and the truncation cap as the size default. */
    private static EqlSearchRequest build(String query, String indices, List<Attribute> schema, Map<String, Object> options) {
        return EqlRequests.build(query, indices, schema, options, null, DEFAULT_ENCLOSING);
    }

    public void testRequiresIndexPattern() {
        EsqlIllegalArgumentException e = expectThrows(
            EsqlIllegalArgumentException.class,
            () -> build("process where true", "  ", NO_SCHEMA, Map.of())
        );
        assertThat(e.getMessage(), containsString("non-empty index pattern"));
    }

    public void testSingleIndexAndQuery() {
        EqlSearchRequest request = build("process where true", "logs-*", NO_SCHEMA, Map.of());
        assertThat(request.indices(), arrayContaining("logs-*"));
        assertThat(request.query(), equalTo("process where true"));
    }

    public void testCommaSeparatedIndicesAreSplitAndTrimmed() {
        EqlSearchRequest request = build("process where true", "logs-a, logs-b ,logs-c", NO_SCHEMA, Map.of());
        assertThat(request.indices(), arrayContaining("logs-a", "logs-b", "logs-c"));
    }

    public void testFetchFieldsFromSchema() {
        List<Attribute> schema = List.of(
            new ReferenceAttribute(EMPTY, "_sequence", LONG), // synthetic — no wire field
            fieldAttribute("process.name", KEYWORD),
            fieldAttribute("@timestamp", DATETIME),
            new UnsupportedAttribute(EMPTY, "blob", new UnsupportedEsField("blob", List.of("binary"), null, Map.of())) // excluded
        );
        EqlSearchRequest request = build("process where true", "logs", schema, Map.of());
        List<FieldAndFormat> fields = request.fetchFields();
        assertThat(fields, hasSize(2));
        assertThat(fields.get(0).field, equalTo("process.name"));
        assertThat(fields.get(0).format, nullValue());
        assertThat(fields.get(1).field, equalTo("@timestamp"));
        assertThat(fields.get(1).format, equalTo("epoch_millis"));
    }

    public void testNullifiedColumnCarriesNoFetchField() {
        // A nullified unmapped column (NULL-typed) produces no value; it must not add a fetch entry.
        List<Attribute> schema = List.of(fieldAttribute("process.name", KEYWORD), fieldAttribute("foo", DataType.NULL));
        EqlSearchRequest request = build("process where true", "logs", schema, Map.of());
        assertThat(request.fetchFields(), hasSize(1));
        assertThat(request.fetchFields().get(0).field, equalTo("process.name"));
    }

    public void testLoadColumnFetchesWithIncludeUnmapped() {
        // A LOAD-mode unmapped column is backed by PotentiallyUnmappedKeywordEsField and must fetch include_unmapped.
        FieldAttribute loaded = new FieldAttribute(EMPTY, "foo", new PotentiallyUnmappedKeywordEsField("foo"));
        List<Attribute> schema = List.of(fieldAttribute("process.name", KEYWORD), loaded);
        EqlSearchRequest request = build("process where true", "logs", schema, Map.of());

        List<FieldAndFormat> fields = request.fetchFields();
        assertThat(fields, hasSize(2));
        FieldAndFormat foo = fields.stream().filter(f -> f.field.equals("foo")).findFirst().orElseThrow();
        assertThat("LOAD column must fetch include_unmapped", foo.includeUnmapped, equalTo(Boolean.TRUE));
        FieldAndFormat name = fields.stream().filter(f -> f.field.equals("process.name")).findFirst().orElseThrow();
        assertThat("mapped column must not set include_unmapped", name.includeUnmapped, nullValue());
    }

    public void testMetadataAttributesCarryNoFetchFields() {
        // Metadata values come from the response envelope, not the fields API, so they must not add fetch entries.
        List<Attribute> schema = List.of(
            fieldAttribute("process.name", KEYWORD),
            MetadataAttribute.create(EMPTY, "_index").toAttribute(),
            MetadataAttribute.create(EMPTY, "_id").toAttribute(),
            MetadataAttribute.create(EMPTY, "_source").toAttribute()
        );
        EqlSearchRequest request = build("process where true", "logs", schema, Map.of());
        List<FieldAndFormat> fields = request.fetchFields();
        assertThat(fields, hasSize(1));
        assertThat(fields.get(0).field, equalTo("process.name"));
    }

    public void testNoFetchFieldsWhenSchemaHasNoMappedFields() {
        List<Attribute> schema = List.of(new ReferenceAttribute(EMPTY, "_sequence", LONG));
        EqlSearchRequest request = build("process where true", "logs", schema, Map.of());
        assertThat(request.fetchFields(), nullValue());
    }

    public void testSizeDefaultsToTruncationCap() {
        // No WITH size, no pushed LIMIT → the cap; and usesTruncationCapSize is true so the caller warns.
        EqlSearchRequest request = EqlRequests.build("process where true", "logs", NO_SCHEMA, Map.of(), null, DEFAULT_ENCLOSING);
        assertThat(request.size(), equalTo(CAP));
        assertThat(EqlRequests.usesTruncationCapSize(Map.of(), null), equalTo(true));
    }

    public void testPushedLimitDrivesSize() {
        EqlSearchRequest request = EqlRequests.build("process where true", "logs", NO_SCHEMA, Map.of(), 5, DEFAULT_ENCLOSING);
        assertThat(request.size(), equalTo(5));
        assertThat(EqlRequests.usesTruncationCapSize(Map.of(), 5), equalTo(false));
    }

    public void testWithSizeWinsOverPushedLimit() {
        // WITH {"size": 7} beats a pushed LIMIT of 5, and disables the truncation warning.
        EqlSearchRequest request = EqlRequests.build("process where true", "logs", NO_SCHEMA, Map.of("size", 7), 5, DEFAULT_ENCLOSING);
        assertThat(request.size(), equalTo(7));
        assertThat(EqlRequests.usesTruncationCapSize(Map.of("size", 7), 5), equalTo(false));
        assertThat(EqlRequests.usesTruncationCapSize(Map.of("size", 7), null), equalTo(false));
    }

    public void testOptionalTuning() {
        EqlSearchRequest request = build(
            "process where true",
            "logs",
            NO_SCHEMA,
            Map.of(
                "size",
                42,
                "fetch_size",
                500,
                "timestamp_field",
                "ts",
                "tiebreaker_field",
                "seq",
                "event_category_field",
                "cat",
                "result_position",
                "head"
            )
        );
        assertThat(request.size(), equalTo(42));
        assertThat(request.fetchSize(), equalTo(500));
        assertThat(request.timestampField(), equalTo("ts"));
        assertThat(request.tiebreakerField(), equalTo("seq"));
        assertThat(request.eventCategoryField(), equalTo("cat"));
        assertThat(request.resultPosition(), equalTo("head"));
    }

    public void testRejectsUnknownOption() {
        ParsingException e = expectThrows(ParsingException.class, () -> EqlRequests.validateOptions(EMPTY, Map.of("sizes", 10)));
        assertThat(e.getMessage(), containsString("unknown EQL command option [sizes]"));
    }

    public void testRejectsWrongTypedOption() {
        ParsingException e = expectThrows(ParsingException.class, () -> EqlRequests.validateOptions(EMPTY, Map.of("size", "10")));
        assertThat(e.getMessage(), containsString("[size] requires a numeric value"));
    }

    public void testRejectsWrongTypedStringOption() {
        // A string-typed option supplied a non-string value must fail loud, not be silently dropped.
        ParsingException e = expectThrows(ParsingException.class, () -> EqlRequests.validateOptions(EMPTY, Map.of("timestamp_field", 5)));
        assertThat(e.getMessage(), containsString("[timestamp_field] requires a string value"));
    }

    public void testRejectsWrongTypedBooleanOption() {
        // The one boolean option: a non-boolean value must fail with the boolean type name, not be coerced or ignored.
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> EqlRequests.validateOptions(EMPTY, Map.of("allow_partial_sequence_results", "true"))
        );
        assertThat(e.getMessage(), containsString("[allow_partial_sequence_results] requires a boolean value"));
    }

    public void testRejectsOversizedNumericOption() {
        // A numeric value above Integer.MAX_VALUE would wrap on intValue() (e.g. size 4294967296 -> 0), presenting an
        // empty result as complete. It must fail at parse time, not truncate silently.
        ParsingException e = expectThrows(ParsingException.class, () -> EqlRequests.validateOptions(EMPTY, Map.of("size", 4294967296L)));
        assertThat(e.getMessage(), allOf(containsString("[size]"), containsString("must be an integer between")));
    }

    public void testRejectsNegativeNumericOptionThatWrapsToNonNegativeInt() {
        // A negative long whose low 32 bits are non-negative also wraps on intValue() (e.g. -4294967296 -> 0),
        // slipping past a one-sided upper-bound check and presenting an empty result as complete.
        ParsingException e = expectThrows(ParsingException.class, () -> EqlRequests.validateOptions(EMPTY, Map.of("size", -4294967296L)));
        assertThat(e.getMessage(), allOf(containsString("[size]"), containsString("must be an integer between")));
    }

    public void testRejectsNegativeNumericOption() {
        ParsingException e = expectThrows(ParsingException.class, () -> EqlRequests.validateOptions(EMPTY, Map.of("size", -1)));
        assertThat(e.getMessage(), allOf(containsString("[size]"), containsString("must be an integer between")));
    }

    public void testRejectsFractionalNumericOption() {
        // A fractional value passes the Number check but would silently truncate on intValue() (3.9 -> 3).
        ParsingException e = expectThrows(ParsingException.class, () -> EqlRequests.validateOptions(EMPTY, Map.of("size", 3.9)));
        assertThat(e.getMessage(), allOf(containsString("[size]"), containsString("must be an integer between")));
    }

    public void testRejectsBelowMinimumFetchSize() {
        // fetch_size below the delegate's minimum (2) must fail at parse, not mid-execution.
        ParsingException e = expectThrows(ParsingException.class, () -> EqlRequests.validateOptions(EMPTY, Map.of("fetch_size", 1)));
        assertThat(e.getMessage(), allOf(containsString("[fetch_size]"), containsString("between 2 and")));
    }

    public void testRejectsBelowMinimumMaxSamplesPerKey() {
        // max_samples_per_key below the delegate's minimum (1) must fail at parse.
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> EqlRequests.validateOptions(EMPTY, Map.of("max_samples_per_key", 0))
        );
        assertThat(e.getMessage(), allOf(containsString("[max_samples_per_key]"), containsString("between 1 and")));
    }

    public void testUnknownOptionMessageListsSupportedKeys() {
        // The unknown-option message enumerates the supported surface so a typo points the user at the real keys.
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> EqlRequests.validateOptions(EMPTY, Map.of("event_category_fields", "category"))
        );
        assertThat(
            e.getMessage(),
            allOf(containsString("unknown EQL command option [event_category_fields]"), containsString("event_category_field"))
        );
    }

    public void testAcceptsEverySupportedOption() {
        // The full option surface validates cleanly with correctly typed values (fails the test if an option is dropped).
        EqlRequests.validateOptions(
            EMPTY,
            Map.of(
                "size",
                1,
                "fetch_size",
                2,
                "max_samples_per_key",
                3,
                "timestamp_field",
                "ts",
                "tiebreaker_field",
                "tb",
                "event_category_field",
                "cat",
                "result_position",
                "tail",
                "allow_partial_sequence_results",
                true
            )
        );
    }

    public void testBridgesPartialSearchResultsFromEnclosingQuery() {
        // An event source inherits the enclosing ES|QL query's partial-results contract rather than a hard pin.
        assertThat(buildWith(new EqlRequests.EnclosingQuery(CAP, true, null, null)).allowPartialSearchResults(), equalTo(true));
        assertThat(buildWith(new EqlRequests.EnclosingQuery(CAP, false, null, null)).allowPartialSearchResults(), equalTo(false));
    }

    public void testSequencePartialResultsDefaultsFalseAndIsOptIn() {
        assertThat(build("sequence [a] [b]", "logs", NO_SCHEMA, Map.of()).allowPartialSequenceResults(), equalTo(false));
        assertThat(
            build("sequence [a] [b]", "logs", NO_SCHEMA, Map.of("allow_partial_sequence_results", true)).allowPartialSequenceResults(),
            equalTo(true)
        );
    }

    public void testBridgesProjectRouting() {
        assertThat(buildWith(new EqlRequests.EnclosingQuery(CAP, false, "_origin:*", null)).getProjectRouting(), equalTo("_origin:*"));
        assertThat(build("process where true", "logs", NO_SCHEMA, Map.of()).getProjectRouting(), nullValue());
    }

    public void testBridgesRequestFilter() {
        MatchAllQueryBuilder filter = new MatchAllQueryBuilder();
        assertThat(buildWith(new EqlRequests.EnclosingQuery(CAP, false, null, filter)).filter(), equalTo(filter));
        assertThat(build("process where true", "logs", NO_SCHEMA, Map.of()).filter(), nullValue());
    }

    public void testMaxSamplesPerKeyOption() {
        assertThat(build("sample by k [a]", "logs", NO_SCHEMA, Map.of("max_samples_per_key", 7)).maxSamplesPerKey(), equalTo(7));
    }

    private static EqlSearchRequest buildWith(EqlRequests.EnclosingQuery enclosing) {
        return EqlRequests.build("process where true", "logs", NO_SCHEMA, Map.of(), null, enclosing);
    }
}
