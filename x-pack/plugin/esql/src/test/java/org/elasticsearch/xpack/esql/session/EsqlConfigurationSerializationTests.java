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

import static org.elasticsearch.xpack.esql.session.EsqlConfiguration.QUERY_COMPRESS_THRESHOLD_CHARS;

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
        int len = randomIntBetween(1, 300) + (frequently() ? 0 : QUERY_COMPRESS_THRESHOLD_CHARS);
        return randomConfiguration(randomRealisticUnicodeOfLength(len));
    }

    public static EsqlConfiguration randomConfiguration(String query) {
        var zoneId = randomZone();
        var locale = randomLocale(random());
        var username = randomAlphaOfLengthBetween(1, 10);
        var clusterName = randomAlphaOfLengthBetween(3, 10);
        var truncation = randomNonNegativeInt();
        var defaultTruncation = randomNonNegativeInt();

        return new EsqlConfiguration(zoneId, locale, username, clusterName, randomQueryPragmas(), truncation, defaultTruncation, query);
    }

    @Override
    protected EsqlConfiguration createTestInstance() {
        return randomConfiguration();
    }

    @Override
    protected EsqlConfiguration mutateInstance(EsqlConfiguration in) throws IOException {
        int ordinal = between(0, 7);
        return new EsqlConfiguration(
            ordinal == 0 ? randomValueOtherThan(in.zoneId(), () -> randomZone().normalized()) : in.zoneId(),
            ordinal == 1 ? randomValueOtherThan(in.locale(), () -> randomLocale(random())) : in.locale(),
            ordinal == 2 ? randomAlphaOfLength(15) : in.username(),
            ordinal == 3 ? randomAlphaOfLength(15) : in.clusterName(),
            ordinal == 4
                ? new QueryPragmas(Settings.builder().put(QueryPragmas.EXCHANGE_BUFFER_SIZE.getKey(), between(1, 10)).build())
                : in.pragmas(),
            ordinal == 5 ? in.resultTruncationMaxSize() + randomIntBetween(3, 10) : in.resultTruncationMaxSize(),
            ordinal == 6 ? in.resultTruncationDefaultSize() + randomIntBetween(3, 10) : in.resultTruncationDefaultSize(),
            ordinal == 7 ? randomAlphaOfLength(100) : in.query()
        );
    }
}
