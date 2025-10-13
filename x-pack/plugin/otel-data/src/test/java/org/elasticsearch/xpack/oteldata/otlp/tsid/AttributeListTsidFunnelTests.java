/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.tsid;

import org.elasticsearch.cluster.routing.TsidBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.util.List;

import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValue;
import static org.elasticsearch.xpack.oteldata.otlp.OtlpUtils.keyValueList;
import static org.hamcrest.Matchers.equalTo;

public class AttributeListTsidFunnelTests extends ESTestCase {

    private final TsidBuilder plainBuilder = new TsidBuilder();
    private final TsidBuilder funnelBuilder = new TsidBuilder();
    private final BufferedByteStringAccessor byteStringAccessor = new BufferedByteStringAccessor();
    private final AttributeListTsidFunnel funnel = AttributeListTsidFunnel.get(byteStringAccessor, "attributes.");

    public void testSimpleAttributes() {
        funnel.add(
            List.of(
                keyValue("string", "value"),
                keyValue("array", "value1", "value2"),
                keyValue("bool", true),
                keyValue("double", 1.5),
                keyValue("long", 42L),
                keyValue("int", 42)
            ),
            funnelBuilder
        );
        plainBuilder.addStringDimension("attributes.string", "value");
        plainBuilder.addStringDimension("attributes.array", "value1");
        plainBuilder.addStringDimension("attributes.array", "value2");
        plainBuilder.addBooleanDimension("attributes.bool", true);
        plainBuilder.addDoubleDimension("attributes.double", 1.5);
        plainBuilder.addLongDimension("attributes.long", 42);
        plainBuilder.addIntDimension("attributes.int", 42);
        assertEquals(plainBuilder.hash(), funnelBuilder.hash());
    }

    public void testNestedAttributes() {
        funnel.add(
            List.of(keyValue("foo", "bar"), keyValue("nested", keyValueList(keyValue("string", "value"), keyValue("int", 42)))),
            funnelBuilder
        );
        plainBuilder.addStringDimension("attributes.nested.string", "value");
        plainBuilder.addLongDimension("attributes.nested.int", 42);
        plainBuilder.addStringDimension("attributes.foo", "bar");
        assertEquals(plainBuilder.hash(), funnelBuilder.hash());
    }

    public void testIgnoredAttributes() {
        funnel.add(
            List.of(
                keyValue("elasticsearch.index", "index"),
                keyValue("data_stream.dataset", "dataset"),
                keyValue("data_stream.namespace", "namespace")
            ),
            funnelBuilder
        );
        assertThat(funnelBuilder.size(), equalTo(0));
    }

}
