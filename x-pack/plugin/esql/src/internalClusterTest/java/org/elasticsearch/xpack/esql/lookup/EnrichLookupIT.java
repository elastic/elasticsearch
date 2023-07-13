/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.lookup;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.Driver;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.DriverRunner;
import org.elasticsearch.compute.operator.OutputOperator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.enrich.EnrichLookupOperator;
import org.elasticsearch.xpack.esql.plugin.TransportEsqlQueryAction;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

public class EnrichLookupIT extends AbstractEsqlIntegTestCase {

    public void testSimple() {
        ElasticsearchAssertions.assertAcked(
            client().admin()
                .indices()
                .prepareCreate("users")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1))
                .setMapping(
                    "uid",
                    "type=keyword,doc_values=false",
                    "name",
                    "type=keyword,index=false",
                    "city",
                    "type=keyword,index=false",
                    "joined",
                    "type=date,index=false,format=yyyy-MM-dd"
                )
        );
        List<Map<String, String>> users = List.of(
            Map.of("uid", "j1", "name", "John", "city", "New York/NY", "joined", "2020-03-01"),
            Map.of("uid", "m4", "name", "Mike", "city", "Boston/MA", "joined", "2010-06-20"),
            Map.of("uid", "j2", "name", "Jack", "city", "Austin/TX", "joined", "1999-11-03")
        );
        for (Map<String, String> user : users) {
            client().prepareIndex("users").setSource(user).get();
            if (randomBoolean()) {
                client().admin().indices().prepareRefresh("users").get();
            }
        }
        if (randomBoolean()) {
            client().admin().indices().prepareForceMerge("users").setMaxNumSegments(1).get();
        }
        client().admin().indices().prepareRefresh("users").get();
        List<NamedExpression> enrichAttributes = List.of(
            new FieldAttribute(Source.EMPTY, "name", new EsField("name", DataTypes.KEYWORD, Map.of(), true)),
            new FieldAttribute(Source.EMPTY, "city", new EsField("city", DataTypes.KEYWORD, Map.of(), true)),
            new FieldAttribute(Source.EMPTY, "joined", new EsField("joined", DataTypes.DATETIME, Map.of(), true))
        );

        DiscoveryNode clientNode = randomFrom(clusterService().state().nodes().stream().toList());
        var lookupService = internalCluster().getInstance(TransportEsqlQueryAction.class, clientNode.getName()).enrichLookupService();
        TransportService transportService = internalCluster().getInstance(TransportService.class, clientNode.getName());

        EsqlQueryRequest parentRequest = new EsqlQueryRequest();
        parentRequest.query("FROM index");
        CancellableTask parentTask = (CancellableTask) transportService.getTaskManager().register("test", "test-action", parentRequest);
        EnrichLookupOperator enrichOperator = new EnrichLookupOperator(
            "test-session",
            parentTask,
            randomIntBetween(1, 3),
            0,
            lookupService,
            "users",
            "match",
            "uid",
            enrichAttributes
        );
        BytesRefBlock userBlock = BytesRefBlock.newBlockBuilder(5)
            .appendBytesRef(new BytesRef("j1"))
            .appendNull()
            .appendBytesRef(new BytesRef("j2"))
            .appendBytesRef(new BytesRef("j1"))
            .appendBytesRef(new BytesRef("m3"))
            .build();
        SourceOperator sourceOperator = sourceOperator(userBlock);

        AtomicReference<Page> outputPage = new AtomicReference<>();
        OutputOperator outputOperator = new OutputOperator(List.of(), Function.identity(), page -> {
            outputPage.getAndUpdate(current -> {
                if (current == null) {
                    return page;
                }
                Block.Builder[] builders = new Block.Builder[current.getBlockCount()];
                for (int i = 0; i < current.getBlockCount(); i++) {
                    ElementType elementType = current.getBlock(i).elementType();
                    if (elementType == ElementType.NULL) {
                        elementType = page.getBlock(i).elementType();
                    }
                    builders[i] = elementType.newBlockBuilder(1);
                    builders[i].copyFrom(current.getBlock(i), 0, current.getPositionCount());
                    builders[i].copyFrom(page.getBlock(i), 0, page.getPositionCount());
                }
                return new Page(Arrays.stream(builders).map(Block.Builder::build).toArray(Block[]::new));
            });
        });

        DateFormatter dateFmt = DateFormatter.forPattern("yyyy-MM-dd");

        DriverRunner.runToCompletion(
            internalCluster().getInstance(TransportService.class).getThreadPool(),
            between(1, 10_000),
            List.of(new Driver(new DriverContext(), sourceOperator, List.of(enrichOperator), outputOperator, () -> {}))
        );
        transportService.getTaskManager().unregister(parentTask);
        Page output = outputPage.get();
        assertThat(output.getBlockCount(), equalTo(4));
        assertThat(output.getPositionCount(), equalTo(5));
        BytesRef scratch = new BytesRef();
        BytesRefBlock names = output.getBlock(1);
        BytesRefBlock cities = output.getBlock(2);
        LongBlock dates = output.getBlock(3);

        assertThat(names.getBytesRef(0, scratch), equalTo(new BytesRef("John")));
        assertThat(cities.getBytesRef(0, scratch), equalTo(new BytesRef("New York/NY")));
        assertThat(dateFmt.formatMillis(dates.getLong(0)), equalTo("2020-03-01"));

        assertTrue(names.isNull(1));
        assertTrue(cities.isNull(1));
        assertTrue(dates.isNull(1));

        assertThat(names.getBytesRef(2, scratch), equalTo(new BytesRef("Jack")));
        assertThat(cities.getBytesRef(2, scratch), equalTo(new BytesRef("Austin/TX")));
        assertThat(dateFmt.formatMillis(dates.getLong(2)), equalTo("1999-11-03"));

        assertThat(names.getBytesRef(3, scratch), equalTo(new BytesRef("John")));
        assertThat(cities.getBytesRef(3, scratch), equalTo(new BytesRef("New York/NY")));
        assertThat(dateFmt.formatMillis(dates.getLong(3)), equalTo("2020-03-01"));

        assertTrue(names.isNull(4));
        assertTrue(cities.isNull(4));
        assertTrue(dates.isNull(4));
    }

    private static SourceOperator sourceOperator(BytesRefBlock input) {
        return new SourceOperator() {
            int position = 0;

            @Override
            public void finish() {

            }

            @Override
            public boolean isFinished() {
                return position >= input.getPositionCount();
            }

            @Override
            public Page getOutput() {
                if (isFinished()) {
                    return null;
                }
                int remaining = input.getPositionCount() - position;
                int size = between(1, remaining);
                BytesRefBlock.Builder builder = BytesRefBlock.newBlockBuilder(size);
                builder.copyFrom(input, position, position + size);
                position += size;
                Block block = builder.build();
                if (block.areAllValuesNull() && randomBoolean()) {
                    block = Block.constantNullBlock(block.getPositionCount());
                }
                return new Page(block);
            }

            @Override
            public void close() {

            }
        };
    }

    public void testRandom() {

    }

    public void testMultipleMatches() {

    }
}
