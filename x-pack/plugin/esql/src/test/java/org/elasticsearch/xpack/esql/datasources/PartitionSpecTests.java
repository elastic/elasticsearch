/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.cluster.metadata.DatasetMapping;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.PartitionSpec.Field;
import org.elasticsearch.xpack.esql.datasources.PartitionSpec.Transform;
import org.elasticsearch.xpack.esql.datasources.PartitionSpec.Unit;
import org.elasticsearch.xpack.esql.datasources.PartitionSpec.Window;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.datasources.PartitionConfig.CONFIG_PARTITIONING_DETECTION;
import static org.elasticsearch.xpack.esql.datasources.PartitionConfig.CONFIG_PARTITIONING_PATH;
import static org.elasticsearch.xpack.esql.datasources.PartitionSpec.CONFIG_PARTITION_SPEC;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class PartitionSpecTests extends ESTestCase {

    public void testParseImplicitTemporalEqualsExplicit() {
        PartitionSpec implicit = PartitionSpec.parse("year(ts), month(ts), day(ts)");
        PartitionSpec explicit = PartitionSpec.parse("year=year(ts), month=month(ts), day=day(ts)");
        assertEquals(explicit, implicit);
        assertEquals(
            List.of(
                new Field("year", Transform.YEAR, "ts", Unit.EPOCH_MILLIS),
                new Field("month", Transform.MONTH, "ts", Unit.EPOCH_MILLIS),
                new Field("day", Transform.DAY, "ts", Unit.EPOCH_MILLIS)
            ),
            implicit.fields()
        );
    }

    public void testParseVpcSecondsImplicitEqualsExplicit() {
        PartitionSpec implicit = PartitionSpec.parse("year(start, epoch_second), month(start, epoch_second), day(start, epoch_second)");
        PartitionSpec explicit = PartitionSpec.parse(
            "year=year(start, epoch_second), month=month(start, epoch_second), day=day(start, epoch_second)"
        );
        assertEquals(explicit, implicit);
        assertEquals(
            List.of(
                new Field("year", Transform.YEAR, "start", Unit.EPOCH_SECOND),
                new Field("month", Transform.MONTH, "start", Unit.EPOCH_SECOND),
                new Field("day", Transform.DAY, "start", Unit.EPOCH_SECOND)
            ),
            implicit.fields()
        );
    }

    public void testParseBareColumnIsIdentity() {
        assertEquals(List.of(new Field("region", Transform.IDENTITY, "region", Unit.EPOCH_MILLIS)), PartitionSpec.parse("region").fields());
    }

    public void testParseAtTimestampAndQuotedName() {
        assertEquals(
            List.of(new Field("year", Transform.YEAR, "@timestamp", Unit.EPOCH_MILLIS)),
            PartitionSpec.parse("year(@timestamp)").fields()
        );
        assertEquals(
            List.of(new Field("year", Transform.YEAR, "event time", Unit.EPOCH_MILLIS)),
            PartitionSpec.parse("year(`event time`)").fields()
        );
        assertEquals(
            List.of(new Field("aws-region", Transform.IDENTITY, "region", Unit.EPOCH_MILLIS)),
            PartitionSpec.parse("`aws-region`=region").fields()
        );
        PartitionSpec.validate(
            Map.of(PartitionConfig.CONFIG_PARTITIONING_DETECTION, "hive", CONFIG_PARTITION_SPEC, "year(@timestamp), month(@timestamp)")
        );
    }

    public void testToSpecStringQuotesDottedNames() {
        PartitionSpec spec = PartitionSpec.parse("year(`event.ts`), lag(`event.ts`, 15m)");
        assertThat(spec.toSpecString(), containsString("`event.ts`"));
        assertEquals(spec, PartitionSpec.parse(spec.toSpecString()));
    }

    public void testAlignWithMappingRoundTripsQuotedNames() {
        DatasetMapping mapping = new DatasetMapping(
            new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, Map.of("event.ts", new DatasetFieldMapping("date", null)))
        );
        PartitionSpec spec = PartitionSpec.parse("year(`event.ts`, epoch_second), lag(`event.ts`, 15m)");
        PartitionSpec aligned = spec.alignWithMapping(mapping);
        assertThat(aligned.toSpecString(), containsString("year(`event.ts`)"));
        assertThat(aligned.toSpecString(), containsString("lag(`event.ts`"));
        assertThat(aligned.toSpecString(), not(containsString("epoch_second")));
        assertEquals(aligned, PartitionSpec.parse(aligned.toSpecString()));
    }

    public void testRejectIdentityAndTemporalOnSameKey() {
        for (String spec : List.of("year=region, year(ts)", "year(ts), year=region")) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PartitionSpec.parse(spec));
            assertThat(e.getMessage(), containsString(CONFIG_PARTITION_SPEC));
            assertThat(e.getMessage(), containsString("year"));
            assertThat(e.getMessage(), containsString("identity"));
        }
    }

    public void testUnusableNoticeForBadSpecAndNone() {
        assertNull(PartitionSpec.unusableNotice(Map.of()));
        assertNull(PartitionSpec.unusableNotice(Map.of(CONFIG_PARTITION_SPEC, "year(ts)")));
        assertThat(PartitionSpec.unusableNotice(Map.of(CONFIG_PARTITION_SPEC, "nope(ts)")), containsString("unknown transform"));
        assertThat(
            PartitionSpec.unusableNotice(Map.of(PartitionConfig.CONFIG_PARTITIONING_DETECTION, "none", CONFIG_PARTITION_SPEC, "year(ts)")),
            containsString("partition detection is disabled")
        );
    }

    public void testParseIdentityRemap() {
        assertEquals(
            List.of(new Field("aws-region", Transform.IDENTITY, "region", Unit.EPOCH_MILLIS)),
            PartitionSpec.parse("aws-region=region").fields()
        );
    }

    public void testParseIdentityCall() {
        assertEquals(
            List.of(new Field("identity", Transform.IDENTITY, "region", Unit.EPOCH_MILLIS)),
            PartitionSpec.parse("identity(region)").fields()
        );
        assertEquals(
            List.of(new Field("aws-region", Transform.IDENTITY, "region", Unit.EPOCH_MILLIS)),
            PartitionSpec.parse("aws-region=identity(region)").fields()
        );
    }

    public void testParseCaseInsensitiveTransformAndUnit() {
        assertEquals(
            List.of(
                new Field("year", Transform.YEAR, "start", Unit.EPOCH_SECOND),
                new Field("month", Transform.MONTH, "event_time", Unit.EPOCH_MILLIS)
            ),
            PartitionSpec.parse("YEAR(start, EPOCH_SECOND), Month(event_time, Epoch_Millis)").fields()
        );
    }

    public void testRejectMixedUnitsOnSameColumn() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PartitionSpec.parse("year(start, epoch_second), month(start)")
        );
        assertThat(e.getMessage(), containsString(CONFIG_PARTITION_SPEC));
        assertThat(e.getMessage(), containsString("start"));
        assertThat(e.getMessage(), containsString("epoch_second"));
        assertThat(e.getMessage(), containsString("epoch_millis"));
        assertThat(e.getMessage(), containsString("one unit per source column"));
    }

    public void testParseKeysAndColumnsAreCaseSensitive() {
        PartitionSpec spec = PartitionSpec.parse("Year=year(Start)");
        assertEquals("Year", spec.fields().get(0).key());
        assertEquals("Start", spec.fields().get(0).column());
    }

    public void testParseSurfacedReservedKey() {
        PartitionSpec spec = PartitionSpec.parse("_partition._index=_partition._index");
        assertEquals("_partition._index", spec.fields().get(0).key());
    }

    public void testParseWhitespaceAroundTokens() {
        PartitionSpec spec = PartitionSpec.parse(" year ( start , epoch_second ) , month ( start , epoch_second ) ");
        assertEquals(2, spec.fields().size());
        assertEquals(Unit.EPOCH_SECOND, spec.fields().get(0).unit());
        assertEquals(Unit.EPOCH_SECOND, spec.fields().get(1).unit());
    }

    public void testParseHourEpochSecond() {
        PartitionSpec spec = PartitionSpec.parse("hour(event_time, epoch_second)");
        assertEquals(new Field("hour", Transform.HOUR, "event_time", Unit.EPOCH_SECOND), spec.fields().get(0));
    }

    public void testFromConfigAbsentIsEmpty() {
        assertSame(PartitionSpec.EMPTY, PartitionSpec.fromConfig(null));
        assertSame(PartitionSpec.EMPTY, PartitionSpec.fromConfig(Map.of()));
        assertSame(PartitionSpec.EMPTY, PartitionSpec.fromConfig(Map.of("partition_detection", "hive")));
    }

    public void testFromConfigUnparseableIsEmpty() {
        assertEquals(PartitionSpec.EMPTY, PartitionSpec.fromConfig(Map.of(CONFIG_PARTITION_SPEC, "bucket(ts)")));
        assertEquals(PartitionSpec.EMPTY, PartitionSpec.fromConfig(Map.of(CONFIG_PARTITION_SPEC, 42)));
        assertEquals(PartitionSpec.EMPTY, PartitionSpec.fromConfig(Map.of(CONFIG_PARTITION_SPEC, "")));
    }

    public void testFromConfigParsesValidSpec() {
        PartitionSpec spec = PartitionSpec.fromConfig(Map.of(CONFIG_PARTITION_SPEC, "year(ts)"));
        assertEquals(1, spec.fields().size());
        assertEquals(Transform.YEAR, spec.fields().get(0).transform());
    }

    public void testValidateAbsentIsOk() {
        PartitionSpec.validate(null);
        PartitionSpec.validate(Map.of());
        PartitionSpec.validate(Map.of("partition_detection", "hive"));
    }

    public void testValidateAcceptsHiveHyphenatedKey() {
        PartitionSpec.validate(Map.of(CONFIG_PARTITIONING_DETECTION, "hive", CONFIG_PARTITION_SPEC, "aws-region=region"));
    }

    public void testValidateAcceptsTemplateKeys() {
        PartitionSpec.validate(
            Map.of(
                CONFIG_PARTITIONING_DETECTION,
                "template",
                CONFIG_PARTITIONING_PATH,
                "{year}/{month}",
                CONFIG_PARTITION_SPEC,
                "year(ts), month(ts)"
            )
        );
        // Spec keys are a subset of placeholders.
        PartitionSpec.validate(Map.of(CONFIG_PARTITIONING_PATH, "{year}/{month}/{day}", CONFIG_PARTITION_SPEC, "year(ts)"));
    }

    public void testValidateAcceptsSpecWithoutExplicitDetection() {
        PartitionSpec.validate(Map.of(CONFIG_PARTITION_SPEC, "year(ts)"));
    }

    public void testValidateHiveSurfacesParseError() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PartitionSpec.validate(Map.of(CONFIG_PARTITIONING_DETECTION, "hive", CONFIG_PARTITION_SPEC, "bucket(ts)"))
        );
        assertThat(e.getMessage(), containsString("unknown transform"));
        assertThat(e.getMessage(), containsString("bucket"));
        assertThat(e.getMessage(), containsString("identity, year, month, day, hour"));
    }

    public void testValidateRejectsNoneEvenWhenUnparseable() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PartitionSpec.validate(Map.of(CONFIG_PARTITIONING_DETECTION, "none", CONFIG_PARTITION_SPEC, "bucket(ts)"))
        );
        assertThat(e.getMessage(), containsString(CONFIG_PARTITION_SPEC));
        assertThat(e.getMessage(), containsString("would be ignored"));
        assertThat(e.getMessage(), containsString("enable partition detection"));
        assertThat(e.getMessage(), containsString("remove [" + CONFIG_PARTITION_SPEC + "]"));
    }

    public void testValidateRejectsNoneWithValidSpec() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PartitionSpec.validate(Map.of(CONFIG_PARTITIONING_DETECTION, "none", CONFIG_PARTITION_SPEC, "year(ts)"))
        );
        assertThat(e.getMessage(), containsString(CONFIG_PARTITION_SPEC));
        assertThat(e.getMessage(), containsString("would be ignored"));
        assertThat(e.getMessage(), containsString("enable partition detection"));
        assertThat(e.getMessage(), containsString("remove [" + CONFIG_PARTITION_SPEC + "]"));
    }

    public void testValidateRejectsTemplateKeyMismatch() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PartitionSpec.validate(Map.of(CONFIG_PARTITIONING_PATH, "{yyy}/{mo}", CONFIG_PARTITION_SPEC, "year(ts)"))
        );
        assertThat(e.getMessage(), containsString(CONFIG_PARTITION_SPEC));
        assertThat(e.getMessage(), containsString("year"));
        assertThat(e.getMessage(), containsString(CONFIG_PARTITIONING_PATH));
        assertThat(e.getMessage(), containsString("{yyy}/{mo}"));
    }

    public void testValidateRejectsHyphenatedKeyAgainstTemplate() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PartitionSpec.validate(Map.of(CONFIG_PARTITIONING_PATH, "{year}/{month}", CONFIG_PARTITION_SPEC, "aws-region=region"))
        );
        assertThat(e.getMessage(), containsString("aws-region"));
        assertThat(e.getMessage(), containsString(CONFIG_PARTITIONING_PATH));
    }

    public void testValidateRejectsNonString() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PartitionSpec.validate(Map.of(CONFIG_PARTITION_SPEC, 42))
        );
        assertThat(e.getMessage(), containsString(CONFIG_PARTITION_SPEC));
        assertThat(e.getMessage(), containsString("non-empty string"));
    }

    public void testValidateRejectsBlank() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PartitionSpec.validate(Map.of(CONFIG_PARTITION_SPEC, "   "))
        );
        assertThat(e.getMessage(), containsString(CONFIG_PARTITION_SPEC));
        assertThat(e.getMessage(), containsString("non-empty string"));
    }

    public void testRejectUnknownTransform() {
        assertReject("bucket(ts)", "bucket", "identity, year, month, day, hour");
        assertReject("truncate(ts)", "truncate", "identity, year, month, day, hour");
    }

    public void testRejectUnknownUnit() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PartitionSpec.parse("year(start, banana)"));
        assertThat(e.getMessage(), containsString(CONFIG_PARTITION_SPEC));
        assertThat(e.getMessage(), containsString("year(start, banana)"));
        assertThat(e.getMessage(), containsString("banana"));
        assertThat(e.getMessage(), containsString("epoch_second, epoch_millis"));
        assertThat(e.getMessage(), containsString("omit the unit"));
    }

    public void testRejectBracedUnitLookalike() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PartitionSpec.parse("year(start, {second})"));
        assertThat(e.getMessage(), containsString("unknown unit"));
        assertThat(e.getMessage(), containsString("year(start, {second})"));
        assertThat(e.getMessage(), containsString("{second}"));
        assertThat(e.getMessage(), containsString("epoch_second, epoch_millis"));
    }

    public void testRejectLegacyUnitTokens() {
        for (String unit : List.of("second", "millis", "micros")) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> PartitionSpec.parse("year(start, " + unit + ")")
            );
            assertThat(e.getMessage(), containsString(CONFIG_PARTITION_SPEC));
            assertThat(e.getMessage(), containsString("unknown unit [" + unit + "]"));
            assertThat(e.getMessage(), containsString("take [epoch_second, epoch_millis]"));
        }
    }

    public void testRejectIdentityWithUnit() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PartitionSpec.parse("identity(region, epoch_millis)")
        );
        assertThat(e.getMessage(), containsString(CONFIG_PARTITION_SPEC));
        assertThat(e.getMessage(), containsString("identity(region, epoch_millis)"));
        assertThat(e.getMessage(), containsString("does not take a unit"));
        assertThat(e.getMessage(), containsString("epoch_millis"));
    }

    public void testRejectMissingClose() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PartitionSpec.parse("year(start"));
        assertThat(e.getMessage(), containsString(CONFIG_PARTITION_SPEC));
        assertThat(e.getMessage(), containsString("year(start"));
        assertThat(e.getMessage(), containsString("closing [)]"));
    }

    public void testRejectLeftoverJunk() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PartitionSpec.parse("year(start) extra"));
        assertThat(e.getMessage(), containsString("leftover text [extra]"));
        assertThat(e.getMessage(), containsString("year(start) extra"));
    }

    public void testRejectEmptySpec() {
        for (String spec : List.of("", "  ", "\t")) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PartitionSpec.parse(spec));
            assertThat(e.getMessage(), containsString(CONFIG_PARTITION_SPEC));
            assertThat(e.getMessage(), containsString("non-empty string"));
        }
    }

    public void testRejectEmptyField() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PartitionSpec.parse("year(start),"));
        assertThat(e.getMessage(), containsString("empty field"));
        assertThat(e.getMessage(), containsString(CONFIG_PARTITION_SPEC));
    }

    public void testRejectDottedIdentifier() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PartitionSpec.parse("year(start.foo)"));
        assertThat(e.getMessage(), containsString("start.foo"));
        assertThat(e.getMessage(), containsString(IDENTIFIER_HINT));
    }

    public void testRejectStarIdentifier() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PartitionSpec.parse("*"));
        assertThat(e.getMessage(), containsString("[*]"));
        assertThat(e.getMessage(), containsString(IDENTIFIER_HINT));
    }

    public void testRejectDedicatedParseErrors() {
        assertReject("year()", "missing a column", "year(column)");
        assertReject("year=", "missing a column after [=]", "key=column");
        assertReject("region=", "missing a column after [=]", "key=transform(column)");
        assertReject("(ts)", "missing a transform name", "[(]");
        assertReject("year(ts,)", "empty argument", "remove the extra comma");
        assertReject("year(, epoch_second)", "empty argument", "remove the extra comma");
        assertReject("year(start))", "stray closing [)]", "field list");
        assertReject("year(ts, epoch_millis, extra)", "leftover text [extra]", "remove [extra]");
        assertReject(",year(ts)", "empty field", "extra comma");
        assertReject("year(ts),,month(ts)", "empty field", "extra comma");
        assertReject("9col", "invalid identifier [9col]", IDENTIFIER_HINT);
        assertReject("year(1ts)", "invalid identifier [1ts]", IDENTIFIER_HINT);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PartitionSpec.parse(null));
        assertThat(e.getMessage(), containsString("non-empty string"));
    }

    public void testAcceptSameKeyOnDifferentColumns() {
        PartitionSpec spec = PartitionSpec.parse(
            "year(start, epoch_second), month(start, epoch_second), day(start, epoch_second), hour(start, epoch_second), "
                + "year(end, epoch_second), month(end, epoch_second), day(end, epoch_second), hour(end, epoch_second), "
                + "lag(start, 20m), lag(end, 10m)"
        );
        assertEquals(8, spec.fields().size());
        assertEquals(TimeValue.timeValueMinutes(20), spec.windows().get("start").lag());
        assertEquals(TimeValue.timeValueMinutes(10), spec.windows().get("end").lag());
    }

    public void testRejectDuplicateKeyOnSameColumn() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PartitionSpec.parse("year(start), year(start)"));
        assertThat(e.getMessage(), containsString(CONFIG_PARTITION_SPEC));
        assertThat(e.getMessage(), containsString("year"));
        assertThat(e.getMessage(), containsString("start"));
        assertThat(e.getMessage(), containsString("once per column"));
    }

    public void testRejectDuplicateIdentityKey() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PartitionSpec.parse("aws-region=region, aws-region=az")
        );
        assertThat(e.getMessage(), containsString(CONFIG_PARTITION_SPEC));
        assertThat(e.getMessage(), containsString("identity key"));
        assertThat(e.getMessage(), containsString("aws-region"));
    }

    public void testParseLagAndLead() {
        PartitionSpec spec = PartitionSpec.parse(
            "year(@timestamp), month(@timestamp), day(@timestamp), hour(@timestamp), lag(@timestamp, 15m)"
        );
        assertEquals(TimeValue.timeValueMinutes(15), spec.windows().get("@timestamp").lag());
        assertEquals(TimeValue.ZERO, spec.windows().get("@timestamp").lead());
        PartitionSpec both = PartitionSpec.parse("year(ts), lag(ts, 1h), lead(ts, 90s)");
        Window window = both.windows().get("ts");
        assertEquals(TimeValue.timeValueHours(1), window.lag());
        assertEquals(TimeValue.timeValueSeconds(90), window.lead());
        assertEquals(both, PartitionSpec.parse(both.toSpecString()));
        PartitionSpec zero = PartitionSpec.parse("year(ts), lag(ts, 0ms)");
        assertEquals(TimeValue.ZERO, zero.windows().get("ts").lag());
    }

    public void testRejectLagWithoutTemporalBind() {
        assertReject("lag(nope, 15m)", "nope", "no temporal bind");
        assertReject("year(ts), lag(nope, 15m)", "nope", "year/month/day/hour");
    }

    public void testRejectNegativeAndUnparseableLag() {
        assertReject("year(ts), lag(ts, -15m)", "negative", "non-negative");
        assertReject("year(ts), lag(ts, banana)", "unparseable duration", "15m");
        assertReject("year(ts), lag(ts, 15m), lag(ts, 1h)", "more than once", "column+direction");
        assertReject("year(ts), key=lag(ts, 15m)", "cannot assign [lag] to a key", "lag(column, duration)");
        assertReject("year(ts), lag(ts)", "lag(column, duration)", "15m");
    }

    public void testConfigKeysIsExactlyPartitionSpec() {
        assertEquals(Set.of(CONFIG_PARTITION_SPEC), PartitionSpec.CONFIG_KEYS);
    }

    public void testAlignWithMappingRejectsPathSourceWhenMapped() {
        DatasetMapping mapping = new DatasetMapping(
            new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, Map.of("@timestamp", new DatasetFieldMapping("date", "start")))
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PartitionSpec.parse("year(start, epoch_second), month(start, epoch_second), lag(start, 15m)").alignWithMapping(mapping)
        );
        assertThat(e.getMessage(), containsString(CONFIG_PARTITION_SPEC));
        assertThat(e.getMessage(), containsString("start"));
        assertThat(e.getMessage(), containsString("@timestamp"));
        assertThat(e.getMessage(), containsString("bind"));
        Map<String, Object> settings = Map.of(CONFIG_PARTITION_SPEC, "year(start, epoch_second)");
        e = expectThrows(IllegalArgumentException.class, () -> PartitionSpec.alignWithMapping(settings, mapping));
        assertThat(e.getMessage(), containsString("bind [@timestamp]"));
    }

    public void testAlignWithMappingDropsUnusedDateUnit() {
        DatasetMapping mapping = new DatasetMapping(
            new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, Map.of("@timestamp", new DatasetFieldMapping("date", "start")))
        );
        PartitionSpec spec = PartitionSpec.parse("year(@timestamp, epoch_second), month(@timestamp, epoch_second), lag(@timestamp, 15m)");
        PartitionSpec aligned = spec.alignWithMapping(mapping);
        assertEquals(
            List.of(
                new Field("year", Transform.YEAR, "@timestamp", Unit.EPOCH_MILLIS),
                new Field("month", Transform.MONTH, "@timestamp", Unit.EPOCH_MILLIS)
            ),
            aligned.fields()
        );
        assertEquals(TimeValue.timeValueMinutes(15), aligned.windows().get("@timestamp").lag());
        assertThat(aligned.toSpecString(), containsString("year(@timestamp)"));
        assertThat(aligned.toSpecString(), not(containsString("epoch_second")));
        Map<String, Object> settings = Map.of(CONFIG_PARTITION_SPEC, "year(@timestamp, epoch_second)");
        assertEquals("year(@timestamp)", PartitionSpec.alignWithMapping(settings, mapping).get(CONFIG_PARTITION_SPEC));
    }

    public void testAlignWithMappingRejectsUnknownColumnWhenMappingPresent() {
        DatasetMapping mapping = new DatasetMapping(
            new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, Map.of("@timestamp", new DatasetFieldMapping("date", null)))
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PartitionSpec.parse("year(nope)").alignWithMapping(mapping)
        );
        assertThat(e.getMessage(), containsString(CONFIG_PARTITION_SPEC));
        assertThat(e.getMessage(), containsString("nope"));
        assertThat(e.getMessage(), containsString("not a mapping field"));
    }

    public void testAlignWithMappingAcceptsNoMapping() {
        PartitionSpec spec = PartitionSpec.parse("year(start, epoch_second)");
        assertEquals(spec, spec.alignWithMapping(null));
        Map<String, Object> settings = Map.of(CONFIG_PARTITION_SPEC, "year(start, epoch_second)");
        assertEquals(settings, PartitionSpec.alignWithMapping(settings, null));
    }

    public void testAlignWithMappingAcceptsIdentityNotInMapping() {
        DatasetMapping mapping = new DatasetMapping(
            new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, Map.of("@timestamp", new DatasetFieldMapping("date", "start")))
        );
        PartitionSpec spec = PartitionSpec.parse("aws-region=region, year(@timestamp, epoch_second)");
        PartitionSpec aligned = spec.alignWithMapping(mapping);
        assertEquals(new Field("aws-region", Transform.IDENTITY, "region", Unit.EPOCH_MILLIS), aligned.fields().get(0));
        assertEquals(new Field("year", Transform.YEAR, "@timestamp", Unit.EPOCH_MILLIS), aligned.fields().get(1));
    }

    public void testAlignWithMappingRejectsPathWhenLogicalAlsoBound() {
        DatasetMapping mapping = new DatasetMapping(
            new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, Map.of("@timestamp", new DatasetFieldMapping("date", "start")))
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PartitionSpec.parse("year(start), year(@timestamp)").alignWithMapping(mapping)
        );
        assertThat(e.getMessage(), containsString("start"));
        assertThat(e.getMessage(), containsString("bind [@timestamp]"));
    }

    public void testAlignWithMappingRejectsLagOnPathSource() {
        DatasetMapping mapping = new DatasetMapping(
            new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, Map.of("@timestamp", new DatasetFieldMapping("date", "start")))
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> PartitionSpec.parse("year(start), hour(start), lag(start, 20m)").alignWithMapping(mapping)
        );
        assertThat(e.getMessage(), containsString("start"));
        assertThat(e.getMessage(), containsString("bind [@timestamp]"));
    }

    public void testAlignWithMappingKeepsComplementaryLagAndLead() {
        DatasetMapping mapping = new DatasetMapping(
            new DatasetMapping.Mappings(DatasetMapping.Dynamic.FALSE, Map.of("@timestamp", new DatasetFieldMapping("date", "start")))
        );
        PartitionSpec aligned = PartitionSpec.parse("year(@timestamp), hour(@timestamp), lag(@timestamp, 20m), lead(@timestamp, 10m)")
            .alignWithMapping(mapping);
        assertEquals(TimeValue.timeValueMinutes(20), aligned.windows().get("@timestamp").lag());
        assertEquals(TimeValue.timeValueMinutes(10), aligned.windows().get("@timestamp").lead());
        assertEquals(aligned, PartitionSpec.parse(aligned.toSpecString()));
    }

    private static final String IDENTIFIER_HINT = PartitionSpec.IDENTIFIER_RULE;

    private static void assertReject(String spec, String badToken, String fix) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PartitionSpec.parse(spec));
        assertThat(e.getMessage(), containsString(CONFIG_PARTITION_SPEC));
        assertThat(e.getMessage(), containsString(badToken));
        assertThat(e.getMessage(), containsString(fix));
    }
}
