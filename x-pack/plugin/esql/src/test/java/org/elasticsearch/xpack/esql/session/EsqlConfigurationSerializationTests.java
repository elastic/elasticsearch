/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;

public class EsqlConfigurationSerializationTests extends AbstractWireSerializingTestCase<EsqlConfiguration> {

    @Override
    protected Writeable.Reader<EsqlConfiguration> instanceReader() {
        return EsqlConfiguration::new;
    }

    private static QueryPragmas randomQueryPragmas() {
        return new QueryPragmas(
            Settings.builder().put(QueryPragmas.DATA_PARTITIONING.getKey(), randomFrom(DataPartitioning.values())).build()
        );
    }

    public static EsqlConfiguration randomConfiguration() {
        var zoneId = randomZone();
        var username = randomAlphaOfLengthBetween(1, 10);
        var clusterName = randomAlphaOfLengthBetween(3, 10);
        var truncation = randomNonNegativeInt();

        return new EsqlConfiguration(zoneId, username, clusterName, randomQueryPragmas(), truncation);
    }

    @Override
    protected EsqlConfiguration createTestInstance() {
        return randomConfiguration();
    }

    @Override
    protected EsqlConfiguration mutateInstance(EsqlConfiguration in) throws IOException {
        int ordinal = between(0, 4);
        return new EsqlConfiguration(
            ordinal == 0 ? randomValueOtherThan(in.zoneId(), () -> randomZone().normalized()) : in.zoneId(),
            ordinal == 1 ? randomAlphaOfLength(15) : in.username(),
            ordinal == 2 ? randomAlphaOfLength(15) : in.clusterName(),
            ordinal == 3
                ? new QueryPragmas(Settings.builder().put(QueryPragmas.EXCHANGE_BUFFER_SIZE.getKey(), between(1, 10)).build())
                : in.pragmas(),
            ordinal == 4 ? in.resultTruncationMaxSize() + randomIntBetween(3, 10) : in.resultTruncationMaxSize()
        );
    }
}
