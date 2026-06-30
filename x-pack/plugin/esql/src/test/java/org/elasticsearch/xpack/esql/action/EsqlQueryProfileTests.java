/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class EsqlQueryProfileTests extends AbstractWireSerializingTestCase<EsqlQueryProfile> {

    @Override
    protected Writeable.Reader<EsqlQueryProfile> instanceReader() {
        return EsqlQueryProfile::readFrom;
    }

    @Override
    protected EsqlQueryProfile createTestInstance() {
        return new EsqlQueryProfile(
            randomTimeSpan(),
            randomTimeSpan(),
            randomTimeSpan(),
            randomTimeSpan(),
            randomTimeSpan(),
            randomTimeSpan(),
            randomTimeSpan(),
            randomTimeSpan(),
            randomTimeSpan(),
            randomTimeSpan(),
            randomIntBetween(0, 100),
            randomIntBetween(0, 100),
            randomIntBetween(0, 1000),
            randomNonNegativeLong(),
            randomIntBetween(0, 100)
        );
    }

    @Override
    protected EsqlQueryProfile mutateInstance(EsqlQueryProfile instance) throws IOException {
        TimeSpan query = instance.total().timeSpan();
        TimeSpan planning = instance.planning().timeSpan();
        TimeSpan parsing = instance.parsing().timeSpan();
        TimeSpan viewResolution = instance.viewResolution().timeSpan();
        TimeSpan datasetResolution = instance.datasetResolution().timeSpan();
        TimeSpan preAnalysis = instance.preAnalysis().timeSpan();
        TimeSpan indicesResolutionMarker = instance.indicesResolutionMarker().timeSpan();
        TimeSpan enrichResolutionMarker = instance.enrichResolutionMarker().timeSpan();
        TimeSpan inferenceResolutionMarker = instance.inferenceResolutionMarker().timeSpan();
        TimeSpan analysis = instance.analysis().timeSpan();
        int fieldCapsCalls = instance.fieldCapsCalls();
        int filesScanned = instance.filesScanned();
        int splitsScanned = instance.splitsScanned();
        long bytesScanned = instance.bytesScanned();
        int externalWarmAggregates = instance.externalWarmAggregates();
        switch (randomIntBetween(0, 14)) {
            case 0 -> query = randomValueOtherThan(query, EsqlQueryProfileTests::randomTimeSpan);
            case 1 -> planning = randomValueOtherThan(planning, EsqlQueryProfileTests::randomTimeSpan);
            case 2 -> parsing = randomValueOtherThan(parsing, EsqlQueryProfileTests::randomTimeSpan);
            case 3 -> viewResolution = randomValueOtherThan(viewResolution, EsqlQueryProfileTests::randomTimeSpan);
            case 4 -> datasetResolution = randomValueOtherThan(datasetResolution, EsqlQueryProfileTests::randomTimeSpan);
            case 5 -> preAnalysis = randomValueOtherThan(preAnalysis, EsqlQueryProfileTests::randomTimeSpan);
            case 6 -> indicesResolutionMarker = randomValueOtherThan(indicesResolutionMarker, EsqlQueryProfileTests::randomTimeSpan);
            case 7 -> enrichResolutionMarker = randomValueOtherThan(enrichResolutionMarker, EsqlQueryProfileTests::randomTimeSpan);
            case 8 -> inferenceResolutionMarker = randomValueOtherThan(inferenceResolutionMarker, EsqlQueryProfileTests::randomTimeSpan);
            case 9 -> analysis = randomValueOtherThan(analysis, EsqlQueryProfileTests::randomTimeSpan);
            case 10 -> fieldCapsCalls = randomValueOtherThan(fieldCapsCalls, () -> randomIntBetween(0, 100));
            case 11 -> filesScanned = randomValueOtherThan(filesScanned, () -> randomIntBetween(0, 100));
            case 12 -> splitsScanned = randomValueOtherThan(splitsScanned, () -> randomIntBetween(0, 1000));
            case 13 -> bytesScanned = randomValueOtherThan(bytesScanned, () -> randomNonNegativeLong());
            case 14 -> externalWarmAggregates = randomValueOtherThan(externalWarmAggregates, () -> randomIntBetween(0, 100));
        }
        return new EsqlQueryProfile(
            query,
            planning,
            parsing,
            viewResolution,
            datasetResolution,
            preAnalysis,
            indicesResolutionMarker,
            enrichResolutionMarker,
            inferenceResolutionMarker,
            analysis,
            fieldCapsCalls,
            filesScanned,
            splitsScanned,
            bytesScanned,
            externalWarmAggregates
        );
    }

    public void testAddExternalScanStatsIsAdditive() {
        EsqlQueryProfile profile = new EsqlQueryProfile();
        profile.addExternalScanStats(2, 5, 1000L);
        profile.addExternalScanStats(3, 7, 500L);
        assertEquals(5, profile.filesScanned());
        assertEquals(12, profile.splitsScanned());
        assertEquals(1500L, profile.bytesScanned());
    }

    public void testScanStatsOnlyEmittedWhenSplitsScanned() throws IOException {
        EsqlQueryProfile withoutScan = new EsqlQueryProfile();
        assertThat(toJson(withoutScan), not(containsString("splits_scanned")));

        EsqlQueryProfile withScan = new EsqlQueryProfile();
        withScan.addExternalScanStats(2, 4, 2048L);
        String json = toJson(withScan);
        assertThat(json, containsString("\"files_scanned\":2"));
        assertThat(json, containsString("\"splits_scanned\":4"));
        assertThat(json, containsString("\"bytes_scanned\":2048"));
    }

    public void testFileAndByteScanStatsOmittedWhenSourceCannotReportThem() throws IOException {
        // Connector sources like Arrow Flight report splits but no file or byte accounting.
        EsqlQueryProfile connectorScan = new EsqlQueryProfile();
        connectorScan.addExternalScanStats(0, 4, 0L);
        String json = toJson(connectorScan);
        assertThat(json, containsString("\"splits_scanned\":4"));
        assertThat(json, not(containsString("files_scanned")));
        assertThat(json, not(containsString("bytes_scanned")));
    }

    public void testWarmAggregatesIsAdditive() {
        EsqlQueryProfile profile = new EsqlQueryProfile();
        profile.addExternalWarmAggregates(2);
        profile.addExternalWarmAggregates(3);
        assertEquals(5, profile.externalWarmAggregates());
    }

    public void testWarmAggregatesOnlyEmittedWhenServedWarm() throws IOException {
        EsqlQueryProfile notWarm = new EsqlQueryProfile();
        assertThat(toJson(notWarm), not(containsString("external_warm_aggregates")));

        EsqlQueryProfile warm = new EsqlQueryProfile();
        warm.addExternalWarmAggregates(2);
        assertThat(toJson(warm), containsString("\"external_warm_aggregates\":2"));
    }

    private static String toJson(EsqlQueryProfile profile) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            profile.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return Strings.toString(builder);
        }
    }

    private static TimeSpan randomTimeSpan() {
        return randomBoolean()
            ? new TimeSpan(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong())
            : null;
    }
}
