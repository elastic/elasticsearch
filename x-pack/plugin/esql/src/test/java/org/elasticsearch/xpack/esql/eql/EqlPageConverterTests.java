/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.eql;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Event;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Hits;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse.Sequence;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.fieldAttribute;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.BOOLEAN;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.VERSION;
import static org.hamcrest.Matchers.containsString;

/**
 * Unit tests for {@link EqlPageConverter}: they build an {@link EqlSearchResponse} (with fields-API values) by
 * hand and assert the produced {@link Page} matches the resolved typed schema, with no client or driver involved.
 * The schema is passed in explicitly, mirroring what the analyzer resolves from field-caps.
 */
public class EqlPageConverterTests extends ESTestCase {

    private static final List<Attribute> SEQUENCE_SYNTHETICS = List.of(
        new ReferenceAttribute(EMPTY, "_sequence", LONG),
        new ReferenceAttribute(EMPTY, "_sequence_stage", INTEGER),
        new ReferenceAttribute(EMPTY, "join_keys", KEYWORD)
    );

    public void testEventModeTypedColumns() {
        List<Attribute> schema = List.of(fieldAttribute("process.name", KEYWORD), fieldAttribute("process.pid", LONG));
        Event e0 = event(Map.of("process.name", List.of("alpha"), "process.pid", List.of(100)));
        Event e1 = event(Map.of("process.name", List.of("beta"), "process.pid", List.of(200)));

        Page page = convert(eventResponse(List.of(e0, e1)), EqlRelation.Mode.EVENT, schema);
        try {
            assertEquals(2, page.getBlockCount());
            assertEquals(2, page.getPositionCount());
            assertBytesRefColumn(page, 0, "alpha", "beta");
            LongBlock pid = page.getBlock(1);
            assertEquals(100L, pid.getLong(0));
            assertEquals(200L, pid.getLong(1));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testDateColumnReadsEpochMillis() {
        List<Attribute> schema = List.of(fieldAttribute("@timestamp", DATETIME));
        // The request asks for the date field with format epoch_millis, so the fields API renders a digit string.
        Event e0 = event(Map.of("@timestamp", List.of("1609459200000")));

        Page page = convert(eventResponse(List.of(e0)), EqlRelation.Mode.EVENT, schema);
        try {
            LongBlock ts = page.getBlock(0);
            assertEquals(1609459200000L, ts.getLong(0));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testAllConvertibleTypeArms() {
        // One column per non-keyword/non-date convertible type, covering both Number- and String-shaped wire values.
        List<Attribute> schema = List.of(
            fieldAttribute("i", INTEGER),
            fieldAttribute("d", DOUBLE),
            fieldAttribute("b", BOOLEAN),
            fieldAttribute("bs", BOOLEAN),
            fieldAttribute("ip", IP),
            fieldAttribute("v", VERSION)
        );
        Event e0 = event(
            Map.of(
                "i",
                List.of(42),          // Integer
                "d",
                List.of(3.5),         // Double
                "b",
                List.of(true),        // Boolean
                "bs",
                List.of("true"),      // String → parsed
                "ip",
                List.of("1.2.3.4"),   // String → encoded IP bytes
                "v",
                List.of("1.2.3")      // String → encoded version bytes
            )
        );

        Page page = convert(eventResponse(List.of(e0)), EqlRelation.Mode.EVENT, schema);
        try {
            assertEquals(42, ((IntBlock) page.getBlock(0)).getInt(0));
            assertEquals(3.5, ((DoubleBlock) page.getBlock(1)).getDouble(0), 0.0);
            assertTrue(((BooleanBlock) page.getBlock(2)).getBoolean(0));
            assertTrue(((BooleanBlock) page.getBlock(3)).getBoolean(0));
            BytesRef scratch = new BytesRef();
            assertEquals(EsqlDataTypeConverter.stringToIP("1.2.3.4"), ((BytesRefBlock) page.getBlock(4)).getBytesRef(0, scratch));
            assertEquals(EsqlDataTypeConverter.stringToVersion("1.2.3"), ((BytesRefBlock) page.getBlock(5)).getBytesRef(0, scratch));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testNullElementInMultivalueIsDropped() {
        List<Attribute> schema = List.of(fieldAttribute("tags", KEYWORD));
        Event e0 = event(Map.of("tags", Arrays.asList("a", null, "b")));

        Page page = convert(eventResponse(List.of(e0)), EqlRelation.Mode.EVENT, schema);
        try {
            BytesRefBlock tags = page.getBlock(0);
            assertEquals(2, tags.getValueCount(0)); // the null element is dropped
            BytesRef scratch = new BytesRef();
            int first = tags.getFirstValueIndex(0);
            assertEquals(new BytesRef("a"), tags.getBytesRef(first, scratch));
            assertEquals(new BytesRef("b"), tags.getBytesRef(first + 1, scratch));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testMultivalueFieldBecomesMultivaluePosition() {
        List<Attribute> schema = List.of(fieldAttribute("tags", KEYWORD));
        Event e0 = event(Map.of("tags", List.of("a", "b", "c")));

        Page page = convert(eventResponse(List.of(e0)), EqlRelation.Mode.EVENT, schema);
        try {
            BytesRefBlock tags = page.getBlock(0);
            assertEquals(3, tags.getValueCount(0));
            BytesRef scratch = new BytesRef();
            int first = tags.getFirstValueIndex(0);
            assertEquals(new BytesRef("a"), tags.getBytesRef(first, scratch));
            assertEquals(new BytesRef("b"), tags.getBytesRef(first + 1, scratch));
            assertEquals(new BytesRef("c"), tags.getBytesRef(first + 2, scratch));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testAbsentFieldBecomesNull() {
        List<Attribute> schema = List.of(fieldAttribute("process.name", KEYWORD), fieldAttribute("process.pid", LONG));
        Event e0 = event(Map.of("process.name", List.of("alpha"))); // no pid

        Page page = convert(eventResponse(List.of(e0)), EqlRelation.Mode.EVENT, schema);
        try {
            assertBytesRefColumn(page, 0, "alpha");
            LongBlock pid = page.getBlock(1);
            assertTrue("absent field must be null", pid.isNull(0));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testUnsupportedColumnIsAllNull() {
        UnsupportedAttribute blob = new UnsupportedAttribute(
            EMPTY,
            "blob",
            new UnsupportedEsField("blob", List.of("binary"), null, Map.of())
        );
        List<Attribute> schema = List.of(fieldAttribute("process.name", KEYWORD), blob);
        Event e0 = event(Map.of("process.name", List.of("alpha"), "blob", List.of("ignored")));

        Page page = convert(eventResponse(List.of(e0)), EqlRelation.Mode.EVENT, schema);
        try {
            assertBytesRefColumn(page, 0, "alpha");
            assertTrue("unsupported column must be all null", page.getBlock(1).areAllValuesNull());
        } finally {
            page.releaseBlocks();
        }
    }

    public void testSequenceModeUnnestsToOneRowPerEventWithSynthetics() {
        List<Attribute> schema = concat(SEQUENCE_SYNTHETICS, fieldAttribute("process.name", KEYWORD));
        Sequence s0 = new Sequence(List.of("host-a"), List.of(fieldEvent("p0"), fieldEvent("n0")));
        Sequence s1 = new Sequence(List.of("host-b"), List.of(fieldEvent("p1"), fieldEvent("n1")));

        Page page = convert(sequenceResponse(List.of(s0, s1)), EqlRelation.Mode.SEQUENCE, schema);
        try {
            assertEquals(4, page.getBlockCount());
            assertEquals(4, page.getPositionCount()); // 2 sequences * 2 events

            LongBlock seq = page.getBlock(0);
            IntBlock stage = page.getBlock(1);
            assertEquals(0L, seq.getLong(0));
            assertEquals(0, stage.getInt(0));
            assertEquals(0L, seq.getLong(1));
            assertEquals(1, stage.getInt(1));
            assertEquals(1L, seq.getLong(2));
            assertEquals(0, stage.getInt(2));
            assertEquals(1L, seq.getLong(3));
            assertEquals(1, stage.getInt(3));

            assertBytesRefColumn(page, 2, "host-a", "host-a", "host-b", "host-b"); // join_keys
            assertBytesRefColumn(page, 3, "p0", "n0", "p1", "n1");                 // process.name
        } finally {
            page.releaseBlocks();
        }
    }

    public void testMissingEventNullsFieldsButKeepsSynthetics() {
        List<Attribute> schema = concat(SEQUENCE_SYNTHETICS, fieldAttribute("process.name", KEYWORD));
        Event present = fieldEvent("p0");
        Event missing = new Event("", "", null, null, true);
        Sequence s0 = new Sequence(List.of("k"), List.of(present, missing));

        Page page = convert(sequenceResponse(List.of(s0)), EqlRelation.Mode.SEQUENCE, schema);
        try {
            LongBlock seq = page.getBlock(0);
            IntBlock stage = page.getBlock(1);
            assertEquals(0L, seq.getLong(1)); // synthetics still populated for the missing event's row
            assertEquals(1, stage.getInt(1));
            BytesRefBlock name = page.getBlock(3);
            assertFalse(name.isNull(0));
            assertTrue("missing event's field must be null", name.isNull(1));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testNullJoinKeyBecomesNullNotString() {
        List<Attribute> schema = concat(SEQUENCE_SYNTHETICS, fieldAttribute("process.name", KEYWORD));
        Sequence s0 = new Sequence(Collections.singletonList(null), List.of(fieldEvent("p0")));

        Page page = convert(sequenceResponse(List.of(s0)), EqlRelation.Mode.SEQUENCE, schema);
        try {
            BytesRefBlock joinKeys = page.getBlock(2);
            assertTrue("a null join key must render as null, not the string \"null\"", joinKeys.isNull(0));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testMetadataColumnsEventMode() {
        List<Attribute> schema = List.of(fieldAttribute("process.name", KEYWORD), metadata("_index"), metadata("_id"), metadata("_source"));
        Event e0 = envelopeEvent("logs-2026", "abc", new BytesArray("{\"a\":1}"), Map.of("process.name", List.of("cmd.exe")));

        Page page = convert(eventResponse(List.of(e0)), EqlRelation.Mode.EVENT, schema);
        try {
            assertBytesRefColumn(page, 0, "cmd.exe");
            assertBytesRefColumn(page, 1, "logs-2026");  // _index — from the envelope, not the fields API
            assertBytesRefColumn(page, 2, "abc");         // _id
            assertBytesRefColumn(page, 3, "{\"a\":1}");   // _source
        } finally {
            page.releaseBlocks();
        }
    }

    public void testMetadataSourceDefaultsToEmptyObject() {
        // The EQL Event constructor normalizes a null source to {}, so the _source column is never null in practice
        // (the converter still null-guards defensively). Pin the normalized empty-object behavior.
        List<Attribute> schema = List.of(metadata("_source"));
        Event e0 = envelopeEvent("logs", "x", null, Map.of());

        Page page = convert(eventResponse(List.of(e0)), EqlRelation.Mode.EVENT, schema);
        try {
            assertBytesRefColumn(page, 0, "{}");
        } finally {
            page.releaseBlocks();
        }
    }

    public void testMissingEventNullsMetadataButKeepsSynthetics() {
        List<Attribute> schema = concat(SEQUENCE_SYNTHETICS, metadata("_id"));
        Event present = envelopeEvent("logs", "p0", new BytesArray("{}"), Map.of());
        Event missing = new Event("", "", null, null, true);
        Sequence s0 = new Sequence(List.of("k"), List.of(present, missing));

        Page page = convert(sequenceResponse(List.of(s0)), EqlRelation.Mode.SEQUENCE, schema);
        try {
            assertEquals(1, ((IntBlock) page.getBlock(1)).getInt(1)); // synthetic populated on the missing row
            BytesRefBlock id = page.getBlock(3);
            assertFalse(id.isNull(0));
            assertTrue("missing event metadata must be null (not \"\")", id.isNull(1));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testMappedFieldNamedLikeMetadataDispatchesByClass() {
        // A mapped field named _index (a FieldAttribute) and the _index metadata column coexist; class dispatch keeps
        // them distinct — the field takes the fields-API value, the metadata takes the envelope value.
        List<Attribute> schema = List.of(fieldAttribute("_index", KEYWORD), metadata("_index"));
        Event e0 = envelopeEvent("from-envelope", "x", new BytesArray("{}"), Map.of("_index", List.of("from-fields-api")));

        Page page = convert(eventResponse(List.of(e0)), EqlRelation.Mode.EVENT, schema);
        try {
            assertBytesRefColumn(page, 0, "from-fields-api");
            assertBytesRefColumn(page, 1, "from-envelope");
        } finally {
            page.releaseBlocks();
        }
    }

    public void testMetadataOnlySchema() {
        List<Attribute> schema = List.of(metadata("_index"), metadata("_id"));
        Event e0 = envelopeEvent("logs", "id1", new BytesArray("{}"), Map.of());

        Page page = convert(eventResponse(List.of(e0)), EqlRelation.Mode.EVENT, schema);
        try {
            assertEquals(2, page.getBlockCount());
            assertEquals(1, page.getPositionCount());
            assertBytesRefColumn(page, 0, "logs");
            assertBytesRefColumn(page, 1, "id1");
        } finally {
            page.releaseBlocks();
        }
    }

    public void testUnexpectedMetadataNameThrows() {
        // _score is a valid FROM metadata field but not an EQL envelope field; the analyzer rejects it, and the
        // converter is a defensive tripwire if one ever slips through.
        List<Attribute> schema = List.of(metadata("_score"));
        EqlSearchResponse response = eventResponse(List.of(envelopeEvent("logs", "x", new BytesArray("{}"), Map.of())));
        try {
            EsqlIllegalArgumentException e = expectThrows(
                EsqlIllegalArgumentException.class,
                () -> EqlPageConverter.toPage(response, EqlRelation.Mode.EVENT, schema, TestBlockFactory.getNonBreakingInstance())
            );
            assertThat(e.getMessage(), containsString("unexpected EQL metadata column"));
        } finally {
            response.decRef();
        }
    }

    public void testNullifiedColumnIsConstantNullEvenIfEventHasValue() {
        // A nullified unmapped column is a NULL-typed FieldAttribute; it must render as all-null and never read data,
        // even if the event happens to carry a value under that name.
        List<Attribute> schema = List.of(fieldAttribute("process.name", KEYWORD), fieldAttribute("foo", DataType.NULL));
        Event e0 = event(Map.of("process.name", List.of("cmd.exe"), "foo", List.of("leaked")));

        Page page = convert(eventResponse(List.of(e0)), EqlRelation.Mode.EVENT, schema);
        try {
            assertBytesRefColumn(page, 0, "cmd.exe");
            assertTrue("nullified column must be all null", page.getBlock(1).areAllValuesNull());
        } finally {
            page.releaseBlocks();
        }
    }

    public void testLoadedUnmappedColumnReadsFieldsApiValue() {
        // A LOAD-mode column is a keyword FieldAttribute backed by PotentiallyUnmappedKeywordEsField; the converter
        // reads it from the fields API exactly like any mapped keyword (class dispatch, no special case).
        FieldAttribute loaded = new FieldAttribute(EMPTY, "foo", new PotentiallyUnmappedKeywordEsField("foo"));
        List<Attribute> schema = List.of(loaded);
        Event present = event(Map.of("foo", List.of("srv-1")));
        Event absent = event(Map.of("process.name", List.of("x"))); // no foo in _source

        Page page = convert(eventResponse(List.of(present, absent)), EqlRelation.Mode.EVENT, schema);
        try {
            BytesRefBlock foo = page.getBlock(0);
            assertEquals(new BytesRef("srv-1"), foo.getBytesRef(foo.getFirstValueIndex(0), new BytesRef()));
            assertTrue("absent-from-_source row must be null", foo.isNull(1));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testNumericArmsParseStringShapedValues() {
        // The fields API can render numbers as strings; every numeric arm must parse the string shape (not blind-cast),
        // covering the String branch of the long/integer/double conversions.
        List<Attribute> schema = List.of(fieldAttribute("l", LONG), fieldAttribute("i", INTEGER), fieldAttribute("d", DOUBLE));
        Event e0 = event(Map.of("l", List.of("100"), "i", List.of("7"), "d", List.of("2.5")));

        Page page = convert(eventResponse(List.of(e0)), EqlRelation.Mode.EVENT, schema);
        try {
            assertEquals(100L, ((LongBlock) page.getBlock(0)).getLong(0));
            assertEquals(7, ((IntBlock) page.getBlock(1)).getInt(0));
            assertEquals(2.5, ((DoubleBlock) page.getBlock(2)).getDouble(0), 0.0);
        } finally {
            page.releaseBlocks();
        }
    }

    public void testSampleModeUnnestsLikeSequence() {
        // SAMPLE routes through the same sequence-unnesting path as SEQUENCE (one row per event, with synthetics);
        // pin that the SAMPLE mode arm produces the join-key groups, since only SEQUENCE exercised it before.
        List<Attribute> schema = concat(SEQUENCE_SYNTHETICS, fieldAttribute("process.name", KEYWORD));
        Sequence s0 = new Sequence(List.of("host-a"), List.of(fieldEvent("p0"), fieldEvent("p1")));

        Page page = convert(sequenceResponse(List.of(s0)), EqlRelation.Mode.SAMPLE, schema);
        try {
            assertEquals(2, page.getPositionCount());
            LongBlock seq = page.getBlock(0);
            IntBlock stage = page.getBlock(1);
            assertEquals(0L, seq.getLong(0));
            assertEquals(0, stage.getInt(0));
            assertEquals(1, stage.getInt(1));
            assertBytesRefColumn(page, 2, "host-a", "host-a"); // join_keys
            assertBytesRefColumn(page, 3, "p0", "p1");         // process.name
        } finally {
            page.releaseBlocks();
        }
    }

    public void testEmptyEventResponseYieldsZeroPositions() {
        // No events (e.g. a matched-nothing event query) must yield a zero-row page under the schema, not throw.
        List<Attribute> schema = List.of(fieldAttribute("process.name", KEYWORD));

        Page page = convert(eventResponse(List.of()), EqlRelation.Mode.EVENT, schema);
        try {
            assertEquals(1, page.getBlockCount());
            assertEquals(0, page.getPositionCount());
        } finally {
            page.releaseBlocks();
        }
    }

    public void testEmptySequenceResponseYieldsZeroPositions() {
        // No sequences must yield a zero-row page including the synthetics columns, not throw.
        List<Attribute> schema = concat(SEQUENCE_SYNTHETICS, fieldAttribute("process.name", KEYWORD));

        Page page = convert(sequenceResponse(List.of()), EqlRelation.Mode.SEQUENCE, schema);
        try {
            assertEquals(4, page.getBlockCount());
            assertEquals(0, page.getPositionCount());
        } finally {
            page.releaseBlocks();
        }
    }

    public void testEmptyJoinKeysRenderNull() {
        // A sequence whose join-keys list is empty (no BY keys) renders the join_keys synthetic as null, not "".
        List<Attribute> schema = concat(SEQUENCE_SYNTHETICS, fieldAttribute("process.name", KEYWORD));
        Sequence s0 = new Sequence(List.of(), List.of(fieldEvent("p0")));

        Page page = convert(sequenceResponse(List.of(s0)), EqlRelation.Mode.SEQUENCE, schema);
        try {
            BytesRefBlock joinKeys = page.getBlock(2);
            assertTrue("empty join keys must render as null", joinKeys.isNull(0));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testEventWithNullFetchFieldsRendersFieldNull() {
        // An event that carries no fields map at all (fetchFields() == null) yields a null in every field column.
        List<Attribute> schema = List.of(fieldAttribute("process.name", KEYWORD));
        Event noFields = new Event("logs", "id0", null, null, false);

        Page page = convert(eventResponse(List.of(noFields)), EqlRelation.Mode.EVENT, schema);
        try {
            assertTrue("field of a fields-less event must be null", page.getBlock(0).isNull(0));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testEmptyFieldValuesRenderNull() {
        // A field present in the event but with an empty values list (nothing extracted) renders as null.
        List<Attribute> schema = List.of(fieldAttribute("process.name", KEYWORD));
        Event e0 = event(Map.of("process.name", List.of()));

        Page page = convert(eventResponse(List.of(e0)), EqlRelation.Mode.EVENT, schema);
        try {
            assertTrue("empty-values field must be null", page.getBlock(0).isNull(0));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testUnexpectedSyntheticNameThrows() {
        // A ReferenceAttribute whose name is not one of the three synthetics is a defensive tripwire in valueFor:
        // the resolver only ever emits _sequence/_sequence_stage/join_keys, so any other synthetic name is a bug.
        List<Attribute> schema = List.of(new ReferenceAttribute(EMPTY, "_bogus_synthetic", LONG));
        EqlSearchResponse response = sequenceResponse(List.of(new Sequence(List.of("k"), List.of(fieldEvent("p0")))));
        try {
            EsqlIllegalArgumentException e = expectThrows(
                EsqlIllegalArgumentException.class,
                () -> EqlPageConverter.toPage(response, EqlRelation.Mode.SEQUENCE, schema, TestBlockFactory.getNonBreakingInstance())
            );
            assertThat(e.getMessage(), containsString("unexpected EQL synthetic column [_bogus_synthetic]"));
        } finally {
            response.decRef();
        }
    }

    public void testCompositeJoinKeysRenderAsMultivalue() {
        // A sequence with more than one BY field carries several join keys; they render as one multivalue join_keys
        // entry, not a single concatenated string. Only single-key sequences were exercised before.
        List<Attribute> schema = concat(SEQUENCE_SYNTHETICS, fieldAttribute("process.name", KEYWORD));
        Sequence s0 = new Sequence(List.of("host-a", "user-b"), List.of(fieldEvent("p0")));

        Page page = convert(sequenceResponse(List.of(s0)), EqlRelation.Mode.SEQUENCE, schema);
        try {
            BytesRefBlock joinKeys = page.getBlock(2);
            assertEquals(2, joinKeys.getValueCount(0));
            BytesRef scratch = new BytesRef();
            int first = joinKeys.getFirstValueIndex(0);
            assertEquals(new BytesRef("host-a"), joinKeys.getBytesRef(first, scratch));
            assertEquals(new BytesRef("user-b"), joinKeys.getBytesRef(first + 1, scratch));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testFieldWithOnlyNullValuesRendersNull() {
        // A field whose values list holds only nulls renders the position as null: norm is never called on a null,
        // and an all-null multivalue collapses to null rather than an empty position.
        List<Attribute> schema = List.of(fieldAttribute("single", KEYWORD), fieldAttribute("multi", KEYWORD));
        Event e0 = event(Map.of("single", Collections.singletonList(null), "multi", Arrays.asList(null, null)));

        Page page = convert(eventResponse(List.of(e0)), EqlRelation.Mode.EVENT, schema);
        try {
            assertTrue("single null value must render null", page.getBlock(0).isNull(0));
            assertTrue("all-null multivalue must render null", page.getBlock(1).isNull(0));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testNonConvertibleFieldTypeThrows() {
        // norm() has a defensive default arm: the resolve-time CONVERTIBLE_TYPES gate turns any non-convertible field
        // into an UnsupportedAttribute before it can reach the converter, so a FieldAttribute of an unconvertible type
        // is a bug. DATE_NANOS has a block element type (so it gets a wrapper) but no norm arm; hand-build one to pin
        // the tripwire.
        List<Attribute> schema = List.of(fieldAttribute("nanos", DataType.DATE_NANOS));
        EqlSearchResponse response = eventResponse(List.of(event(Map.of("nanos", List.of(123)))));
        try {
            EsqlIllegalArgumentException e = expectThrows(
                EsqlIllegalArgumentException.class,
                () -> EqlPageConverter.toPage(response, EqlRelation.Mode.EVENT, schema, TestBlockFactory.getNonBreakingInstance())
            );
            assertThat(e.getMessage(), containsString("EQL command cannot convert"));
        } finally {
            response.decRef();
        }
    }

    private static MetadataAttribute metadata(String name) {
        return (MetadataAttribute) MetadataAttribute.create(EMPTY, name);
    }

    private static Event envelopeEvent(String index, String id, BytesReference source, Map<String, ? extends List<?>> fields) {
        Map<String, DocumentField> fetched = new HashMap<>();
        for (Map.Entry<String, ? extends List<?>> e : fields.entrySet()) {
            @SuppressWarnings("unchecked")
            List<Object> values = (List<Object>) e.getValue();
            fetched.put(e.getKey(), new DocumentField(e.getKey(), values));
        }
        return new Event(index, id, source, fetched, false);
    }

    private static Page convert(EqlSearchResponse response, EqlRelation.Mode mode, List<Attribute> schema) {
        Page page = EqlPageConverter.toPage(response, mode, schema, TestBlockFactory.getNonBreakingInstance());
        response.decRef();
        return page;
    }

    private static Event event(Map<String, ? extends List<?>> fields) {
        Map<String, DocumentField> fetched = new HashMap<>();
        for (Map.Entry<String, ? extends List<?>> e : fields.entrySet()) {
            @SuppressWarnings("unchecked")
            List<Object> values = (List<Object>) e.getValue();
            fetched.put(e.getKey(), new DocumentField(e.getKey(), values));
        }
        return new Event("logs", randomAlphaOfLength(4), null, fetched, false);
    }

    private static Event fieldEvent(String name) {
        return event(Map.of("process.name", List.of(name)));
    }

    private static List<Attribute> concat(List<Attribute> head, Attribute tail) {
        return Stream.concat(head.stream(), Stream.of(tail)).toList();
    }

    private static EqlSearchResponse eventResponse(List<Event> events) {
        Hits hits = new Hits(events, null, new TotalHits(events.size(), TotalHits.Relation.EQUAL_TO));
        return new EqlSearchResponse(hits, 1, false, noFailures());
    }

    private static EqlSearchResponse sequenceResponse(List<Sequence> sequences) {
        Hits hits = new Hits(null, sequences, new TotalHits(sequences.size(), TotalHits.Relation.EQUAL_TO));
        return new EqlSearchResponse(hits, 1, false, noFailures());
    }

    private static ShardSearchFailure[] noFailures() {
        return new ShardSearchFailure[0];
    }

    private static void assertBytesRefColumn(Page page, int blockIndex, String... expected) {
        BytesRefBlock block = page.getBlock(blockIndex);
        BytesRef scratch = new BytesRef();
        for (int i = 0; i < expected.length; i++) {
            assertEquals("row " + i + " of block " + blockIndex, new BytesRef(expected[i]), block.getBytesRef(i, scratch));
        }
    }
}
