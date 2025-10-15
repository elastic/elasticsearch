/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.ExpressionWritables;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class QuerySettingsSerializationTests extends AbstractWireSerializingTestCase<QuerySettings> {

    @Override
    protected Writeable.Reader<QuerySettings> instanceReader() {
        return QuerySettings::new;
    }

    @Override
    protected QuerySettings createTestInstance() {
        return randomSettings();
    }

    @Override
    protected QuerySettings mutateInstance(QuerySettings in) {
        return randomValueOtherThan(in, QuerySettingsSerializationTests::randomSettings);
    }

    private static final Map<QuerySettings.QuerySettingDef<?>, Supplier<Literal>> SETTINGS_GENERATORS = Map.of(
        QuerySettings.PROJECT_ROUTING,
        () -> Literal.keyword(Source.EMPTY, randomAlphaOfLength(15)),
        QuerySettings.TIME_ZONE,
        () -> Literal.keyword(Source.EMPTY, randomZone().normalized().toString())
    );

    public static QuerySettings randomSettings() {
        var settings = new HashMap<QuerySettings.QuerySettingDef<?>, Literal>();

        for (var settingGenerator : SETTINGS_GENERATORS.entrySet()) {
            var settingDef = settingGenerator.getKey();
            var settingValueSupplier = settingGenerator.getValue();

            if (randomBoolean()) {
                settings.remove(settingDef);
            } else {
                settings.put(settingDef, settingValueSupplier.get());
            }
        }

        return new QuerySettings(settings);
    }

    @Override
    protected final NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(ExpressionWritables.getNamedWriteables());
    }
}
