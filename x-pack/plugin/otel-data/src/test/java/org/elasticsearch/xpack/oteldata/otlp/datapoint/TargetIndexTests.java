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
import static org.hamcrest.Matchers.sameInstance;

public class TargetIndexTests extends ESTestCase {

    public void testEvaluateWithExplicitIndex() {
        List<KeyValue> attributes = List.of(
            createStringAttribute("elasticsearch.index", "custom-index"),
            createStringAttribute("data_stream.dataset", "should-be-ignored"),
            createStringAttribute("data_stream.namespace", "should-be-ignored")
        );

        TargetIndex index = resolve("metrics", attributes, List.of(), List.of());

        assertThat(index.index(), equalTo("custom-index"));
        assertThat(index.isDataStream(), is(false));
        assertThat(index.type(), nullValue());
        assertThat(index.dataset(), nullValue());
        assertThat(index.namespace(), nullValue());
    }

    public void testEvaluateWithLowerPrecedenceExplicitIndexOverridesDataStreamAttributes() {
        TargetIndex index = resolve(
            "logs",
            List.of(
                createStringAttribute("data_stream.dataset", "custom-dataset"),
                createStringAttribute("data_stream.namespace", "custom-namespace")
            ),
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

        TargetIndex index = resolve("metrics", attributes, List.of(), List.of());

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

        TargetIndex index = resolve("metrics", List.of(), scopeAttributes, List.of());

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

        TargetIndex index = resolve("metrics", List.of(), List.of(), resourceAttributes);

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

        TargetIndex index = resolve("metrics", attributes, scopeAttributes, resourceAttributes);

        // DataStream.sanitizeDataset replaces hyphens with underscores
        assertThat(index.index(), equalTo("metrics-attr_dataset.otel-scope-namespace"));
        assertThat(index.isDataStream(), is(true));
        assertThat(index.type(), equalTo("metrics"));
        assertThat(index.dataset(), equalTo("attr_dataset.otel"));
        assertThat(index.namespace(), equalTo("scope-namespace"));
    }

    public void testEvaluateWithReceiverInScopeName() {
        TargetIndex index = resolve(
            "metrics",
            List.of(),
            InstrumentationScope.newBuilder().setName("some.scope.name/receiver/hostmetrics-receiver").build(),
            List.of()
        );

        assertThat(index.index(), equalTo("metrics-hostmetrics_receiver.otel-default"));
        assertThat(index.isDataStream(), is(true));
        assertThat(index.type(), equalTo("metrics"));
        assertThat(index.dataset(), equalTo("hostmetrics_receiver.otel"));
        assertThat(index.namespace(), equalTo("default"));
    }

    public void testEvaluateWithDefaultValues() {
        TargetIndex index = resolve("metrics", List.of(), List.of(), List.of());

        assertThat(index.index(), equalTo("metrics-generic.otel-default"));
        assertThat(index.isDataStream(), is(true));
        assertThat(index.type(), equalTo("metrics"));
        assertThat(index.dataset(), equalTo("generic.otel"));
        assertThat(index.namespace(), equalTo("default"));
    }

    public void testExemplarsTarget() {
        TargetIndex metrics = resolve("metrics", List.of(), List.of(), List.of());

        TargetIndex exemplars = metrics.exemplarsTarget();

        assertThat(exemplars.index(), equalTo("exemplars-generic.otel-default"));
        assertThat(exemplars.type(), equalTo("exemplars"));
        assertThat(exemplars.dataset(), equalTo("generic.otel"));
        assertThat(exemplars.namespace(), equalTo("default"));
    }

    public void testExplicitIndexHasNoExemplarsTarget() {
        TargetIndex index = resolve("metrics", List.of(createStringAttribute("elasticsearch.index", "custom-index")), List.of(), List.of());

        assertThat(index.exemplarsTarget(), nullValue());
    }

    public void testDataStreamSanitization() {
        List<KeyValue> attributes = List.of(
            createStringAttribute("data_stream.dataset", "Some-Dataset"),
            createStringAttribute("data_stream.namespace", "Some*Namespace")
        );

        TargetIndex index = resolve("metrics", attributes, List.of(), List.of());

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
        TargetIndex receiverIndex = resolve(
            "logs",
            List.of(),
            InstrumentationScope.newBuilder().setName("some.scope.name/receiver/receiver/should/be/ignored").build(),
            List.of()
        );
        TargetIndex connectorIndex = resolve(
            "logs",
            List.of(),
            InstrumentationScope.newBuilder().setName("some.scope.name/connector/connector/should/be/ignored").build(),
            List.of()
        );
        TargetIndex shortReceiverIndex = resolve(
            "logs",
            List.of(),
            InstrumentationScope.newBuilder().setName("/receiver/foo").build(),
            List.of()
        );
        TargetIndex shortConnectorIndex = resolve(
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
        TargetIndex hyphenatedReceiverIndex = resolve(
            "logs",
            List.of(),
            InstrumentationScope.newBuilder().setName("some.scope.name/receiver/host-metricsreceiver/should/be/ignored").build(),
            List.of()
        );

        assertThat(hyphenatedReceiverIndex.index(), equalTo("logs-host_metricsreceiver.otel-default"));
    }

    public void testEvaluateIgnoresEmptyEncodingFormat() {
        TargetIndex index = resolve(
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
        TargetIndex index = resolve(
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
        TargetIndex index = resolve("logs", List.of(createStringAttribute("data_stream.dataset", longDataset)), List.of(), List.of());

        assertThat(index.dataset(), equalTo("a".repeat(95) + ".otel"));
        assertThat(index.dataset().length(), equalTo(100));
    }

    public void testDefaults() {
        assertThat(TargetIndex.defaultForMetrics().index(), equalTo("metrics-generic.otel-default"));
        assertThat(TargetIndex.defaultForLogs().index(), equalTo("logs-generic.otel-default"));
        assertThat(TargetIndex.defaultForTraces().index(), equalTo("traces-generic.otel-default"));
        assertThat(TargetIndex.defaultForMetrics(), sameInstance(TargetIndex.defaultForMetrics()));
    }

    public void testExemplarsTargetIsCached() {
        TargetIndex metrics = TargetIndex.defaultForMetrics();
        assertThat(metrics.exemplarsTarget(), sameInstance(metrics.exemplarsTarget()));
    }

    public void testWithAttributeOverridesReturnsSameInstanceIfNoAttributeApplies() {
        TargetIndex target = TargetIndex.defaultForMetrics();
        assertThat(target.withAttributeOverrides(List.of()), sameInstance(target));
        assertThat(target.withAttributeOverrides(List.of(createStringAttribute("foo", "bar"))), sameInstance(target));

        TargetIndex explicit = target.withAttributeOverrides(List.of(createStringAttribute("elasticsearch.index", "custom-index")));
        assertThat(explicit.withAttributeOverrides(List.of(createStringAttribute("data_stream.dataset", "foo"))), sameInstance(explicit));
        assertThat(explicit.withAttributeOverrides(List.of(createStringAttribute("data_stream.namespace", "foo"))), sameInstance(explicit));
    }

    public void testMoreSpecificExplicitIndexOverridesLessSpecificExplicitIndex() {
        TargetIndex index = resolve(
            "metrics",
            List.of(createStringAttribute("elasticsearch.index", "data-point-index")),
            List.of(),
            List.of(createStringAttribute("elasticsearch.index", "resource-index"))
        );
        assertThat(index.index(), equalTo("data-point-index"));
    }

    public void testLessSpecificExplicitIndexOverridesMoreSpecificDataStreamAttributes() {
        TargetIndex index = resolve(
            "metrics",
            List.of(createStringAttribute("data_stream.dataset", "ds"), createStringAttribute("data_stream.namespace", "ns")),
            List.of(),
            List.of(createStringAttribute("elasticsearch.index", "resource-index"))
        );
        assertThat(index.index(), equalTo("resource-index"));
        assertThat(index.isDataStream(), is(false));
    }

    public void testMaybeApplyScopeBasedRouting() {
        InstrumentationScope scope = InstrumentationScope.newBuilder().setName("go.opentelemetry.io/collector/service").build();

        TargetIndex scopeBased = TargetIndex.defaultForMetrics()
            .withAttributeOverrides(List.of(createStringAttribute("data_stream.namespace", "ns")))
            .maybeApplyScopeBasedRouting(scope);
        assertThat(scopeBased.index(), equalTo("metrics-collectortelemetry.otel-ns"));

        TargetIndex dataset = TargetIndex.defaultForMetrics()
            .withAttributeOverrides(List.of(createStringAttribute("data_stream.dataset", "ds")));
        assertThat(dataset.maybeApplyScopeBasedRouting(scope), sameInstance(dataset));

        TargetIndex explicit = TargetIndex.defaultForMetrics()
            .withAttributeOverrides(List.of(createStringAttribute("elasticsearch.index", "custom-index")));
        assertThat(explicit.maybeApplyScopeBasedRouting(scope), sameInstance(explicit));

        TargetIndex noRouting = TargetIndex.defaultForMetrics();
        assertThat(noRouting.maybeApplyScopeBasedRouting(InstrumentationScope.getDefaultInstance()), sameInstance(noRouting));
    }

    public void testResourceDatasetTakesPrecedenceOverScopeBasedRouting() {
        TargetIndex index = resolve(
            "metrics",
            List.of(),
            InstrumentationScope.newBuilder().setName("go.opentelemetry.io/collector/service").build(),
            List.of(createStringAttribute("data_stream.dataset", "resource-dataset"))
        );
        assertThat(index.index(), equalTo("metrics-resource_dataset.otel-default"));
    }

    public void testDataPointDatasetTakesPrecedenceOverScopeBasedRouting() {
        TargetIndex index = resolve(
            "metrics",
            List.of(createStringAttribute("data_stream.dataset", "attr-dataset")),
            InstrumentationScope.newBuilder().setName("go.opentelemetry.io/collector/service").build(),
            List.of(createStringAttribute("data_stream.namespace", "resource-namespace"))
        );
        assertThat(index.index(), equalTo("metrics-attr_dataset.otel-resource-namespace"));
    }

    public void testExemplarsTargetPreservesDataset() {
        TargetIndex metrics = TargetIndex.defaultForMetrics()
            .withAttributeOverrides(
                List.of(createStringAttribute("data_stream.dataset", "ds"), createStringAttribute("data_stream.namespace", "ns"))
            );
        assertThat(metrics.exemplarsTarget().index(), equalTo("exemplars-ds.otel-ns"));
    }

    private void assertEvaluatedTargetIndex(String type, String scopeName, List<KeyValue> scopeAttributes, String expectedDataset) {
        TargetIndex index = resolve(
            type,
            List.of(),
            InstrumentationScope.newBuilder().setName(scopeName).addAllAttributes(scopeAttributes).build(),
            List.of()
        );

        assertThat(index.index(), equalTo(type + "-" + expectedDataset + ".otel-default"));
        assertThat(index.dataset(), equalTo(expectedDataset + ".otel"));
    }

    private TargetIndex resolve(String type, List<KeyValue> attributes, List<KeyValue> scopeAttributes, List<KeyValue> resourceAttributes) {
        return resolve(type, attributes, InstrumentationScope.newBuilder().addAllAttributes(scopeAttributes).build(), resourceAttributes);
    }

    /**
     * Resolves the target index the same way the OTLP endpoints do: resource, then scope, then record.
     */
    private TargetIndex resolve(String type, List<KeyValue> attributes, InstrumentationScope scope, List<KeyValue> resourceAttributes) {
        return defaultFor(type).withAttributeOverrides(resourceAttributes)
            .withAttributeOverrides(scope.getAttributesList())
            .maybeApplyScopeBasedRouting(scope)
            .withAttributeOverrides(attributes);
    }

    private static TargetIndex defaultFor(String type) {
        return switch (type) {
            case TargetIndex.TYPE_METRICS -> TargetIndex.defaultForMetrics();
            case TargetIndex.TYPE_LOGS -> TargetIndex.defaultForLogs();
            case TargetIndex.TYPE_TRACES -> TargetIndex.defaultForTraces();
            default -> throw new IllegalArgumentException("unexpected type [" + type + "]");
        };
    }

    private KeyValue createStringAttribute(String key, String value) {
        return KeyValue.newBuilder().setKey(key).setValue(AnyValue.newBuilder().setStringValue(value).build()).build();
    }
}
