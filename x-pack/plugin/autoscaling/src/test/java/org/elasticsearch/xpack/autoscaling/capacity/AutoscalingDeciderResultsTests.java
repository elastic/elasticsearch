/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.capacity;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class AutoscalingDeciderResultsTests extends AutoscalingTestCase {

    public void testAutoscalingDeciderResultsRejectsEmptyResults() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new AutoscalingDeciderResults(
                new AutoscalingCapacity(randomAutoscalingResources(), randomAutoscalingResources()),
                randomNodes(),
                new TreeMap<>()
            )
        );
        assertThat(e.getMessage(), equalTo("results can not be empty"));
    }

    public void testRequiredCapacity() {
        AutoscalingCapacity single = randomBoolean() ? randomAutoscalingCapacity() : null;
        verifyRequiredCapacity(single, single);
        // any undecided decider nulls out any required capacities
        verifyRequiredCapacity(null, single, null);
        verifyRequiredCapacity(null, null, single);

        boolean node = randomBoolean();
        boolean storage = randomBoolean();
        boolean memory = randomBoolean();
        boolean processor = randomBoolean() || (storage == false && memory == false);
        AutoscalingCapacity large = randomCapacity(node, storage, memory, processor, 1000, 2000);

        List<AutoscalingCapacity> autoscalingCapacities = new ArrayList<>();
        autoscalingCapacities.add(large);
        IntStream.range(0, 10).mapToObj(i -> randomCapacity(node, storage, memory, processor, 1, 1000)).forEach(autoscalingCapacities::add);

        Randomness.shuffle(autoscalingCapacities);
        verifyRequiredCapacity(large, autoscalingCapacities.toArray(AutoscalingCapacity[]::new));

        AutoscalingCapacity largerStorage = randomCapacity(node, true, false, false, 2000, 3000);
        verifySingleMetricLarger(node, largerStorage, large, large, autoscalingCapacities, largerStorage);

        AutoscalingCapacity largerMemory = randomCapacity(node, false, true, false, 2000, 3000);
        verifySingleMetricLarger(node, large, largerMemory, large, autoscalingCapacities, largerMemory);

        AutoscalingCapacity largerProcessor = randomCapacity(node, false, false, true, 2000, 3000);
        verifySingleMetricLarger(node, large, large, largerProcessor, autoscalingCapacities, largerProcessor);
    }

    public void testToXContent() {
        AutoscalingDeciderResults results = randomAutoscalingDeciderResults();
        Map<String, Object> map = toMap(results);
        boolean hasRequiredCapacity = results.requiredCapacity() != null;
        Set<String> roots = hasRequiredCapacity
            ? Set.of("current_capacity", "current_nodes", "deciders", "required_capacity")
            : Set.of("current_capacity", "current_nodes", "deciders");
        assertThat(map.keySet(), equalTo(roots));
        assertThat(map.get("current_nodes"), instanceOf(List.class));
        List<?> expectedNodes = results.currentNodes().stream().map(dn -> Map.of("name", dn.getName())).collect(Collectors.toList());
        assertThat(map.get("current_nodes"), equalTo(expectedNodes));
        if (hasRequiredCapacity) {
            assertThat(map.get("required_capacity"), equalTo(toMap(results.requiredCapacity())));
        }

        assertThat(map.get("current_capacity"), equalTo(toMap(results.currentCapacity())));
        assertThat(
            map.get("deciders"),
            equalTo(results.results().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> toMap(e.getValue()))))
        );
    }

    public Map<String, Object> toMap(ToXContent tox) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            if (tox.isFragment()) {
                builder.startObject();
            }
            tox.toXContent(builder, null);
            if (tox.isFragment()) {
                builder.endObject();
            }
            try (
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(builder).streamInput())
            ) {
                return parser.map();
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private void verifySingleMetricLarger(
        boolean node,
        AutoscalingCapacity expectedStorage,
        AutoscalingCapacity expectedMemory,
        AutoscalingCapacity expectedProcessor,
        List<AutoscalingCapacity> other,
        AutoscalingCapacity larger
    ) {
        List<AutoscalingCapacity> autoscalingCapacities = new ArrayList<>(other);
        autoscalingCapacities.add(larger);
        Randomness.shuffle(autoscalingCapacities);
        AutoscalingCapacity.Builder expectedBuilder = AutoscalingCapacity.builder()
            .total(expectedStorage.total().storage(), expectedMemory.total().memory(), expectedProcessor.total().processors());
        if (node) {
            expectedBuilder.node(expectedStorage.node().storage(), expectedMemory.node().memory(), expectedProcessor.node().processors());
        }
        verifyRequiredCapacity(expectedBuilder.build(), autoscalingCapacities.toArray(AutoscalingCapacity[]::new));
    }

    private void verifyRequiredCapacity(AutoscalingCapacity expected, AutoscalingCapacity... capacities) {
        AtomicInteger uniqueGenerator = new AtomicInteger();
        SortedMap<String, AutoscalingDeciderResult> results = Arrays.stream(capacities)
            .map(AutoscalingDeciderResultsTests::randomAutoscalingDeciderResultWithCapacity)
            .collect(
                Collectors.toMap(k -> randomAlphaOfLength(10) + "-" + uniqueGenerator.incrementAndGet(), Function.identity(), (a, b) -> {
                    throw new UnsupportedOperationException();
                }, TreeMap::new)
            );
        assertThat(
            new AutoscalingDeciderResults(randomAutoscalingCapacity(), randomNodes(), results).requiredCapacity(),
            equalTo(expected)
        );
    }

    private AutoscalingCapacity randomCapacity(boolean node, boolean storage, boolean memory, boolean processor, int lower, int upper) {
        AutoscalingCapacity.Builder builder = AutoscalingCapacity.builder();
        builder.total(
            storage ? randomLongBetween(lower, upper) : null,
            memory ? randomLongBetween(lower, upper) : null,
            processor ? (double) randomIntBetween(lower, upper) : null
        );
        if (node) {
            builder.node(
                storage ? randomLongBetween(lower, upper) : null,
                memory ? randomLongBetween(lower, upper) : null,
                processor ? (double) randomIntBetween(lower, upper) : null
            );
        }
        return builder.build();
    }
}
