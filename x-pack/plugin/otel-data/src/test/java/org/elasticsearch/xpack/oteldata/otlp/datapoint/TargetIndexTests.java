/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.datapoint;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
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

    public void testEvaluateWithLowerPrecedenceExplicitIndexOverridesDataStreamAttributes() {
        TargetIndex index = TargetIndex.evaluate(
            "logs",
            List.of(
                createStringAttribute("data_stream.dataset", "custom-dataset"),
                createStringAttribute("data_stream.namespace", "custom-namespace")
            ),
            null,
            List.of(createStringAttribute("elasticsearch.index", "scope-index")),
            List.of()
        );

        assertThat(index.index(), equalTo("scope-index"));
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

    public void testEvaluateUsesSelfTelemetryScopeRouting() {
        assertEvaluatedTargetIndex("logs", "go.opentelemetry.io/collector/receiver/receiverhelper", List.of(), "collectortelemetry");
        assertEvaluatedTargetIndex("metrics", "go.opentelemetry.io/collector/receiver/receiverhelper", List.of(), "collectortelemetry");
        assertEvaluatedTargetIndex("traces", "go.opentelemetry.io/collector/receiver/receiverhelper", List.of(), "collectortelemetry");
    }

    public void testEvaluateUsesEncodingFormatBeforeReceiverRouting() {
        assertEvaluatedTargetIndex(
            "logs",
            "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/cpuscraper",
            List.of(createStringAttribute("encoding.format", "aws.cloudtrail")),
            "aws.cloudtrail"
        );
        assertEvaluatedTargetIndex(
            "metrics",
            "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/cpuscraper",
            List.of(createStringAttribute("encoding.format", "aws.cloudtrail")),
            "aws.cloudtrail"
        );
        assertEvaluatedTargetIndex(
            "traces",
            "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/cpuscraper",
            List.of(createStringAttribute("encoding.format", "aws.cloudtrail")),
            "aws.cloudtrail"
        );
    }

    public void testEvaluateUsesReceiverRouting() {
        assertEvaluatedTargetIndex(
            "logs",
            "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/cpuscraper",
            List.of(),
            "hostmetricsreceiver"
        );
        assertEvaluatedTargetIndex(
            "metrics",
            "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/cpuscraper",
            List.of(),
            "hostmetricsreceiver"
        );
        assertEvaluatedTargetIndex(
            "traces",
            "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/cpuscraper",
            List.of(),
            "hostmetricsreceiver"
        );
    }

    public void testEvaluateUsesConnectorRouting() {
        assertEvaluatedTargetIndex(
            "logs",
            "github.com/open-telemetry/opentelemetry-collector-contrib/connector/spanmetricsconnector",
            List.of(),
            "spanmetricsconnector"
        );
        assertEvaluatedTargetIndex(
            "metrics",
            "github.com/open-telemetry/opentelemetry-collector-contrib/connector/spanmetricsconnector",
            List.of(),
            "spanmetricsconnector"
        );
        assertEvaluatedTargetIndex(
            "traces",
            "github.com/open-telemetry/opentelemetry-collector-contrib/connector/spanmetricsconnector",
            List.of(),
            "spanmetricsconnector"
        );
    }

    public void testEvaluateIgnoresComponentRoutingNamesWithoutRequiredSuffix() {
        TargetIndex receiverIndex = evaluateWithScope(
            "logs",
            List.of(),
            InstrumentationScope.newBuilder().setName("some.scope.name/receiver/receiver/should/be/ignored").build(),
            List.of()
        );
        TargetIndex connectorIndex = evaluateWithScope(
            "logs",
            List.of(),
            InstrumentationScope.newBuilder().setName("some.scope.name/connector/connector/should/be/ignored").build(),
            List.of()
        );
        TargetIndex shortReceiverIndex = evaluateWithScope(
            "logs",
            List.of(),
            InstrumentationScope.newBuilder().setName("/receiver/foo").build(),
            List.of()
        );
        TargetIndex shortConnectorIndex = evaluateWithScope(
            "logs",
            List.of(),
            InstrumentationScope.newBuilder().setName("/connector/bar").build(),
            List.of()
        );

        assertThat(receiverIndex.index(), equalTo("logs-generic.otel-default"));
        assertThat(connectorIndex.index(), equalTo("logs-generic.otel-default"));
        assertThat(shortReceiverIndex.index(), equalTo("logs-generic.otel-default"));
        assertThat(shortConnectorIndex.index(), equalTo("logs-generic.otel-default"));
    }

    public void testEvaluateUsesComponentRoutingWithRequiredSuffix() {
        TargetIndex hyphenatedReceiverIndex = evaluateWithScope(
            "logs",
            List.of(),
            InstrumentationScope.newBuilder().setName("some.scope.name/receiver/host-metricsreceiver/should/be/ignored").build(),
            List.of()
        );

        assertThat(hyphenatedReceiverIndex.index(), equalTo("logs-host_metricsreceiver.otel-default"));
    }

    public void testEvaluateIgnoresEmptyEncodingFormat() {
        TargetIndex index = evaluateWithScope(
            "logs",
            List.of(),
            InstrumentationScope.newBuilder()
                .setName("github.com/open-telemetry/opentelemetry-collector-contrib/extension/encoding/awslogsencodingextension")
                .addAttributes(createStringAttribute("encoding.format", ""))
                .build(),
            List.of()
        );

        assertThat(index.index(), equalTo("logs-generic.otel-default"));
        assertThat(index.dataset(), equalTo("generic.otel"));
    }

    public void testEvaluateRespectsExplicitDatasetAttributes() {
        TargetIndex index = evaluateWithScope(
            "logs",
            List.of(createStringAttribute("data_stream.dataset", "attr-dataset")),
            InstrumentationScope.newBuilder()
                .setName(
                    "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hostmetricsreceiver/internal/scraper/cpuscraper"
                )
                .addAttributes(createStringAttribute("encoding.format", "aws.cloudtrail"))
                .build(),
            List.of(createStringAttribute("data_stream.namespace", "resource-namespace"))
        );

        assertThat(index.index(), equalTo("logs-attr_dataset.otel-resource-namespace"));
        assertThat(index.dataset(), equalTo("attr_dataset.otel"));
        assertThat(index.namespace(), equalTo("resource-namespace"));
    }

    public void testEvaluateTruncatesDatasetBeforeAddingOtelSuffix() {
        String longDataset = "a".repeat(100);
        TargetIndex index = TargetIndex.evaluate(
            "logs",
            List.of(createStringAttribute("data_stream.dataset", longDataset)),
            null,
            List.of(),
            List.of()
        );

        assertThat(index.dataset(), equalTo("a".repeat(95) + ".otel"));
        assertThat(index.dataset().length(), equalTo(100));
    }

    private void assertEvaluatedTargetIndex(String type, String scopeName, List<KeyValue> scopeAttributes, String expectedDataset) {
        TargetIndex index = evaluateWithScope(
            type,
            List.of(),
            InstrumentationScope.newBuilder().setName(scopeName).addAllAttributes(scopeAttributes).build(),
            List.of()
        );

        assertThat(index.index(), equalTo(type + "-" + expectedDataset + ".otel-default"));
        assertThat(index.dataset(), equalTo(expectedDataset + ".otel"));
    }

    private TargetIndex evaluateWithScope(
        String type,
        List<KeyValue> attributes,
        InstrumentationScope scope,
        List<KeyValue> resourceAttributes
    ) {
        return TargetIndex.evaluate(
            type,
            attributes,
            TargetIndex.extractScopeRoutingDataset(scope),
            scope.getAttributesList(),
            resourceAttributes
        );
    }

    private KeyValue createStringAttribute(String key, String value) {
        return KeyValue.newBuilder().setKey(key).setValue(AnyValue.newBuilder().setStringValue(value).build()).build();
    }
}
