/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ExemplarsSettingsTests extends AbstractWireSerializingTestCase<ExemplarsSettings> {

    public void testParseBoolean() {
        assertThat(ExemplarsSettings.parse(Literal.TRUE), equalTo(ExemplarsSettings.ENABLED));
        assertThat(ExemplarsSettings.parse(Literal.FALSE), equalTo(ExemplarsSettings.DISABLED));
    }

    public void testParseMapWithLimit() {
        assertThat(ExemplarsSettings.parse(mapExpression("limit", 1234)), equalTo(new ExemplarsSettings(true, 1234, false)));
    }

    public void testParseMapWithIncludeTimeseries() {
        assertThat(
            ExemplarsSettings.parse(mapExpression("include_timeseries", Literal.TRUE)),
            equalTo(new ExemplarsSettings(true, null, true))
        );
        assertThat(
            ExemplarsSettings.parse(mapExpression("include_timeseries", Literal.FALSE)),
            equalTo(new ExemplarsSettings(true, null, false))
        );
    }

    public void testParseMapIncludeTimeseriesNotABoolean() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ExemplarsSettings.parse(mapExpression("include_timeseries", 1))
        );
        assertThat(e.getMessage(), containsString("[include_timeseries] must be a boolean value"));
    }

    public void testParseEmptyMapEnablesWithoutLimit() {
        assertThat(ExemplarsSettings.parse(new MapExpression(Source.EMPTY, List.of())), equalTo(ExemplarsSettings.ENABLED));
    }

    public void testParseMapLimitNotPositive() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ExemplarsSettings.parse(mapExpression("limit", randomIntBetween(Integer.MIN_VALUE, 0)))
        );
        assertThat(e.getMessage(), containsString("[limit] must be positive"));
    }

    public void testParseMapLimitNotAnInteger() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ExemplarsSettings.parse(mapExpression("limit", Literal.keyword(Source.EMPTY, "many")))
        );
        assertThat(e.getMessage(), containsString("[limit] must be an integer value"));
    }

    public void testParseMapUnknownKey() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ExemplarsSettings.parse(mapExpression("unknown_key", 42))
        );
        assertThat(e.getMessage(), containsString("unknown key [unknown_key]"));
    }

    public void testParseInvalidExpression() {
        expectThrows(IllegalArgumentException.class, () -> ExemplarsSettings.parse(Literal.keyword(Source.EMPTY, "not_valid")));
    }

    public void testFromXContent() throws IOException {
        assertThat(fromJson("true"), equalTo(ExemplarsSettings.ENABLED));
        assertThat(fromJson("false"), equalTo(ExemplarsSettings.DISABLED));
        assertThat(fromJson("{\"limit\": 1234}"), equalTo(new ExemplarsSettings(true, 1234, false)));
        assertThat(fromJson("{\"include_timeseries\": true}"), equalTo(new ExemplarsSettings(true, null, true)));
        assertThat(fromJson("{\"limit\": 1234, \"include_timeseries\": true}"), equalTo(new ExemplarsSettings(true, 1234, true)));
        assertThat(fromJson("{}"), equalTo(ExemplarsSettings.ENABLED));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> fromJson("{\"unknown_key\": 42}"));
        assertThat(e.getMessage(), containsString("unknown key [unknown_key]"));
    }

    /**
     * A peer that predates the parameter is written the setting without it and reads it as off. The configuration of every query
     * carries this setting, not only that of an exemplars query, so a mismatch in its format fails all of them.
     */
    public void testIncludeTimeseriesDroppedBeforeItsTransportVersion() throws IOException {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("esql_exemplars_include_timeseries"));
        ExemplarsSettings roundTripped = copyInstance(new ExemplarsSettings(true, 1234, true), before);
        assertThat(roundTripped, equalTo(new ExemplarsSettings(true, 1234, false)));
    }

    private ExemplarsSettings fromJson(String json) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken();
            return ExemplarsSettings.fromXContent(parser);
        }
    }

    private static MapExpression mapExpression(String key, Object value) {
        Expression valueExpression = value instanceof Expression expression
            ? expression
            : new Literal(Source.EMPTY, value, DataType.INTEGER);
        return new MapExpression(Source.EMPTY, List.of(new Literal(Source.EMPTY, new BytesRef(key), DataType.KEYWORD), valueExpression));
    }

    @Override
    protected Writeable.Reader<ExemplarsSettings> instanceReader() {
        return ExemplarsSettings::new;
    }

    @Override
    protected ExemplarsSettings createTestInstance() {
        return randomBoolean()
            ? ExemplarsSettings.DISABLED
            : new ExemplarsSettings(true, randomBoolean() ? null : randomIntBetween(1, 10000), randomBoolean());
    }

    @Override
    protected ExemplarsSettings mutateInstance(ExemplarsSettings instance) {
        if (instance.enabled() == false) {
            return new ExemplarsSettings(true, randomBoolean() ? null : randomIntBetween(1, 10000), randomBoolean());
        }
        Integer otherLimit = instance.limit() == null ? randomIntBetween(1, 10000) : null;
        return switch (between(0, 2)) {
            case 0 -> ExemplarsSettings.DISABLED;
            case 1 -> new ExemplarsSettings(true, otherLimit, instance.includeTimeseries());
            case 2 -> new ExemplarsSettings(true, instance.limit(), instance.includeTimeseries() == false);
            default -> throw new AssertionError("unreachable");
        };
    }
}
