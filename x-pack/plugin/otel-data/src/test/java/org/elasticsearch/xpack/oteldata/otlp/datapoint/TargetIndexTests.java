/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class TargetIndexTests extends ESTestCase {

    public void testEvaluateWithExplicitIndex() {
        List<KeyValue> attributes = List.of(
            createStringAttribute("elasticsearch.index", "custom-index"),
            createStringAttribute("data_stream.dataset", "should-be-ignored"),
            createStringAttribute("data_stream.namespace", "should-be-ignored")
        );

        TargetIndex index = TargetIndex.evaluate("metrics", attributes, null, List.of(), List.of());

        assertThat(index.index(), equalTo("custom-index"));
        assertThat(index.isDataStream(), is(false));
        assertThat(index.type(), nullValue());
        assertThat(index.dataset(), nullValue());
        assertThat(index.namespace(), nullValue());
    }

    public void testEvaluateWithDataStreamAttributes() {
        List<KeyValue> attributes = List.of(
            createStringAttribute("data_stream.dataset", "custom-dataset"),
            createStringAttribute("data_stream.namespace", "custom-namespace")
        );

        TargetIndex index = TargetIndex.evaluate("metrics", attributes, null, List.of(), List.of());

        // DataStream.sanitizeDataset replaces hyphens with underscores
        assertThat(index.index(), equalTo("metrics-custom_dataset.otel-custom-namespace"));
        assertThat(index.isDataStream(), is(true));
        assertThat(index.type(), equalTo("metrics"));
        assertThat(index.dataset(), equalTo("custom_dataset.otel"));
        assertThat(index.namespace(), equalTo("custom-namespace"));
    }

    public void testEvaluateWithScopeAttributes() {
        List<KeyValue> scopeAttributes = List.of(
            createStringAttribute("data_stream.dataset", "scope-dataset"),
            createStringAttribute("data_stream.namespace", "scope-namespace")
        );

        TargetIndex index = TargetIndex.evaluate("metrics", List.of(), null, scopeAttributes, List.of());

        // DataStream.sanitizeDataset replaces hyphens with underscores
        assertThat(index.index(), equalTo("metrics-scope_dataset.otel-scope-namespace"));
        assertThat(index.isDataStream(), is(true));
        assertThat(index.type(), equalTo("metrics"));
        assertThat(index.dataset(), equalTo("scope_dataset.otel"));
        assertThat(index.namespace(), equalTo("scope-namespace"));
    }

    public void testEvaluateWithResourceAttributes() {
        List<KeyValue> resourceAttributes = List.of(
            createStringAttribute("data_stream.dataset", "resource-dataset"),
            createStringAttribute("data_stream.namespace", "resource-namespace")
        );

        TargetIndex index = TargetIndex.evaluate("metrics", List.of(), null, List.of(), resourceAttributes);

        // DataStream.sanitizeDataset replaces hyphens with underscores
        assertThat(index.index(), equalTo("metrics-resource_dataset.otel-resource-namespace"));
        assertThat(index.isDataStream(), is(true));
        assertThat(index.type(), equalTo("metrics"));
        assertThat(index.dataset(), equalTo("resource_dataset.otel"));
        assertThat(index.namespace(), equalTo("resource-namespace"));
    }

    public void testAttributePrecedence() {
        // The order of precedence should be: attributes > scopeAttributes > resourceAttributes
        List<KeyValue> attributes = List.of(createStringAttribute("data_stream.dataset", "attr-dataset"));

        List<KeyValue> scopeAttributes = List.of(
            createStringAttribute("data_stream.dataset", "scope-dataset"),
            createStringAttribute("data_stream.namespace", "scope-namespace")
        );

        List<KeyValue> resourceAttributes = List.of(
            createStringAttribute("data_stream.dataset", "resource-dataset"),
            createStringAttribute("data_stream.namespace", "resource-namespace")
        );

        TargetIndex index = TargetIndex.evaluate("metrics", attributes, null, scopeAttributes, resourceAttributes);

        // DataStream.sanitizeDataset replaces hyphens with underscores
        assertThat(index.index(), equalTo("metrics-attr_dataset.otel-scope-namespace"));
        assertThat(index.isDataStream(), is(true));
        assertThat(index.type(), equalTo("metrics"));
        assertThat(index.dataset(), equalTo("attr_dataset.otel"));
        assertThat(index.namespace(), equalTo("scope-namespace"));
    }

    public void testEvaluateWithReceiverInScopeName() {
        TargetIndex index = TargetIndex.evaluate("metrics", List.of(), "hostmetrics-receiver", List.of(), List.of());

        assertThat(index.index(), equalTo("metrics-hostmetrics_receiver.otel-default"));
        assertThat(index.isDataStream(), is(true));
        assertThat(index.type(), equalTo("metrics"));
        assertThat(index.dataset(), equalTo("hostmetrics_receiver.otel"));
        assertThat(index.namespace(), equalTo("default"));
    }

    public void testEvaluateWithDefaultValues() {
        TargetIndex index = TargetIndex.evaluate("metrics", List.of(), null, List.of(), List.of());

        assertThat(index.index(), equalTo("metrics-generic.otel-default"));
        assertThat(index.isDataStream(), is(true));
        assertThat(index.type(), equalTo("metrics"));
        assertThat(index.dataset(), equalTo("generic.otel"));
        assertThat(index.namespace(), equalTo("default"));
    }

    public void testDataStreamSanitization() {
        List<KeyValue> attributes = List.of(
            createStringAttribute("data_stream.dataset", "Some-Dataset"),
            createStringAttribute("data_stream.namespace", "Some*Namespace")
        );

        TargetIndex index = TargetIndex.evaluate("metrics", attributes, null, List.of(), List.of());

        // DataStream.sanitizeDataset and DataStream.sanitizeNamespace should be applied
        assertThat(index.dataset(), equalTo("some_dataset.otel"));
        assertThat(index.namespace(), equalTo("some_namespace"));
    }

    private KeyValue createStringAttribute(String key, String value) {
        return KeyValue.newBuilder().setKey(key).setValue(AnyValue.newBuilder().setStringValue(value).build()).build();
    }
}
