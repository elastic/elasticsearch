/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.pivot.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.core.transform.transforms.pivot.DateHistogramGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.ScriptConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class DateHistogramGroupSourceTests extends AbstractResponseTestCase<
    DateHistogramGroupSource,
    org.elasticsearch.client.transform.transforms.pivot.DateHistogramGroupSource> {

    public static ScriptConfig randomScriptConfig() {
        ScriptType type = randomFrom(ScriptType.values());
        String lang = randomBoolean() ? Script.DEFAULT_SCRIPT_LANG : randomAlphaOfLengthBetween(1, 20);
        String idOrCode = randomAlphaOfLengthBetween(1, 20);
        Map<String, Object> params = Collections.emptyMap();

        type = ScriptType.STORED;

        Script script = new Script(type, type == ScriptType.STORED ? null : lang, idOrCode, params);
        LinkedHashMap<String, Object> source = null;

        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            XContentBuilder content = script.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            source = (LinkedHashMap<String, Object>) XContentHelper.convertToMap(BytesReference.bytes(content), true, XContentType.JSON)
                .v2();
        } catch (IOException e) {
            // should not happen
            fail("failed to create random script config");
        }
        return new ScriptConfig(source, script);
    }

    public static DateHistogramGroupSource randomDateHistogramGroupSource() {
        String field = randomBoolean() ? null : randomAlphaOfLengthBetween(1, 20);
        ScriptConfig scriptConfig = randomBoolean() ? null : randomScriptConfig();
        DateHistogramGroupSource dateHistogramGroupSource;
        if (randomBoolean()) {
            dateHistogramGroupSource = new DateHistogramGroupSource(
                field,
                scriptConfig,
                randomBoolean(),
                new DateHistogramGroupSource.FixedInterval(new DateHistogramInterval(randomTimeValue(1, 100, "d", "h", "ms", "s", "m"))),
                randomBoolean() ? randomZone() : null
            );
        } else {
            dateHistogramGroupSource = new DateHistogramGroupSource(
                field,
                scriptConfig,
                randomBoolean(),
                new DateHistogramGroupSource.CalendarInterval(new DateHistogramInterval(randomTimeValue(1, 1, "m", "h", "d", "w"))),
                randomBoolean() ? randomZone() : null
            );
        }

        return dateHistogramGroupSource;
    }

    @Override
    protected DateHistogramGroupSource createServerTestInstance(XContentType xContentType) {
        return randomDateHistogramGroupSource();
    }

    @Override
    protected org.elasticsearch.client.transform.transforms.pivot.DateHistogramGroupSource doParseToClientInstance(XContentParser parser) {
        return org.elasticsearch.client.transform.transforms.pivot.DateHistogramGroupSource.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        DateHistogramGroupSource serverTestInstance,
        org.elasticsearch.client.transform.transforms.pivot.DateHistogramGroupSource clientInstance
    ) {
        assertThat(serverTestInstance.getField(), equalTo(clientInstance.getField()));
        if (serverTestInstance.getScriptConfig() != null) {
            assertThat(serverTestInstance.getScriptConfig().getScript(), equalTo(clientInstance.getScript()));
        } else {
            assertNull(clientInstance.getScript());
        }
        assertSameInterval(serverTestInstance.getInterval(), clientInstance.getInterval());
        assertThat(serverTestInstance.getTimeZone(), equalTo(clientInstance.getTimeZone()));
        assertThat(serverTestInstance.getType().name(), equalTo(clientInstance.getType().name()));
    }

    private void assertSameInterval(
        DateHistogramGroupSource.Interval serverTestInstance,
        org.elasticsearch.client.transform.transforms.pivot.DateHistogramGroupSource.Interval clientInstance
    ) {
        assertEquals(serverTestInstance.getName(), clientInstance.getName());
        assertEquals(serverTestInstance.getInterval(), clientInstance.getInterval());
    }

}
