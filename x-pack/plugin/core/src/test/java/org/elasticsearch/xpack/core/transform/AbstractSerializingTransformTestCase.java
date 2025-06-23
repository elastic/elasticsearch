/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.BaseAggregationBuilder;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContent.Params;
import org.elasticsearch.xpack.core.transform.transforms.NullRetentionPolicyConfig;
import org.elasticsearch.xpack.core.transform.transforms.RetentionPolicyConfig;
import org.elasticsearch.xpack.core.transform.transforms.SyncConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeRetentionPolicyConfig;
import org.elasticsearch.xpack.core.transform.transforms.TimeSyncConfig;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;

public abstract class AbstractSerializingTransformTestCase<T extends ToXContent & Writeable> extends AbstractXContentSerializingTestCase<
    T> {

    protected static Params TO_XCONTENT_PARAMS = new ToXContent.MapParams(
        Collections.singletonMap(TransformField.FOR_INTERNAL_STORAGE, "true")
    );

    private NamedWriteableRegistry namedWriteableRegistry;
    private NamedXContentRegistry namedXContentRegistry;

    @Before
    public void registerNamedObjects() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());

        List<NamedWriteableRegistry.Entry> namedWriteables = searchModule.getNamedWriteables();
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(QueryBuilder.class, MockDeprecatedQueryBuilder.NAME, MockDeprecatedQueryBuilder::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                AggregationBuilder.class,
                MockDeprecatedAggregationBuilder.NAME,
                MockDeprecatedAggregationBuilder::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(SyncConfig.class, TransformField.TIME.getPreferredName(), TimeSyncConfig::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                RetentionPolicyConfig.class,
                TransformField.TIME.getPreferredName(),
                TimeRetentionPolicyConfig::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                RetentionPolicyConfig.class,
                NullRetentionPolicyConfig.NAME.getPreferredName(),
                in -> NullRetentionPolicyConfig.INSTANCE
            )
        );

        List<NamedXContentRegistry.Entry> namedXContents = searchModule.getNamedXContents();
        namedXContents.add(
            new NamedXContentRegistry.Entry(
                QueryBuilder.class,
                new ParseField(MockDeprecatedQueryBuilder.NAME),
                (p, c) -> MockDeprecatedQueryBuilder.fromXContent(p)
            )
        );
        namedXContents.add(
            new NamedXContentRegistry.Entry(
                BaseAggregationBuilder.class,
                new ParseField(MockDeprecatedAggregationBuilder.NAME),
                (p, c) -> MockDeprecatedAggregationBuilder.fromXContent(p)
            )
        );

        namedXContents.addAll(new TransformNamedXContentProvider().getNamedXContentParsers());

        namedWriteableRegistry = new NamedWriteableRegistry(namedWriteables);
        namedXContentRegistry = new NamedXContentRegistry(namedXContents);
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return namedXContentRegistry;
    }

    protected <X extends Writeable, Y extends Writeable> Y writeAndReadBWCObject(
        X original,
        NamedWriteableRegistry registry,
        Writeable.Writer<X> writer,
        Writeable.Reader<Y> reader,
        TransportVersion version
    ) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setTransportVersion(version);
            original.writeTo(output);

            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), getNamedWriteableRegistry())) {
                in.setTransportVersion(version);
                return reader.read(in);
            }
        }
    }

}
