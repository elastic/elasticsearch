/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.lucene;

import io.netty.handler.codec.http.HttpMethod;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RequestOptions.Builder;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class LegacyGeoShapeWithDocValuesQueryCompatibilityIT extends FullClusterRestartIndexCompatibilityTestCase {

    public static final Builder IGNORE_WARNINGS = RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE);
    private static final Logger logger = LogManager.getLogger(LegacyGeoShapeWithDocValuesQueryCompatibilityIT.class);
    private static final String[] PREFIX_TREES = new String[] { "geohash", "quadtree" };
    private static final String defaultFieldName = "geo";
    private static final String repository = "LegacyGeoShapeWithDocValuesQueryCompatibilityIT_repository".toLowerCase(Locale.ROOT);
    private static final String snapshot = "LegacyGeoShapeWithDocValuesQueryCompatibilityIT_snapshot".toLowerCase(Locale.ROOT);

    static {
        clusterConfig = config -> config.setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.searchable.snapshot.shared_cache.size", "16MB")
            .setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB");
    }

    private static boolean repoRegistered = false;
    private static boolean snapshotCreated = false;

    public LegacyGeoShapeWithDocValuesQueryCompatibilityIT(Version version) {
        super(version);
    }

    protected void createMapping(String indexName, String fieldName, Settings settings) throws Exception {
        final XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(fieldName)
            .field("type", "geo_shape")
            .field("tree", randomFrom(PREFIX_TREES))
            .endObject()
            .endObject()
            .endObject();

        final Settings finalSetting;
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> createIndex(indexName, settings, Strings.toString(xcb))
        );
        assertThat(
            ex.getMessage(),
            containsString("using deprecated parameters [tree] in mapper [" + fieldName + "] of type [geo_shape] is no longer allowed")
        );
        IndexVersion version = IndexVersionUtils.randomPreviousCompatibleVersion(random(), IndexVersions.V_8_0_0);
        finalSetting = settings(version).put(settings).build();
        createIndex(indexName, finalSetting, Strings.toString(xcb));
    }

    private void createIndex(String indexName, ToXContent settings, ToXContent mapping) throws IOException {
        final Request createIndex = newXContentRequest(HttpMethod.PUT, "/" + indexName, (builder, params) -> {
            builder.startObject("settings");
            settings.toXContent(builder, params);
            builder.endObject();
            mapping.toXContent(builder, params);
            return builder;
        });

        createIndex.setOptions(IGNORE_WARNINGS);
        client().performRequest(createIndex);
        ensureGreen(indexName);
    }

    public void testPointsOnlyExplicit() throws Exception {
        ToXContent mapping = (b, p) -> b.startObject("mappings")
            .startObject("properties")
            .startObject(defaultFieldName)
            .field("type", "geo_shape")
            .field("tree", randomBoolean() ? "quadtree" : "geohash")
            .field("tree_levels", "6")
            .field("distance_error_pct", "0.01")
            .field("points_only", true)
            .endObject()
            .endObject()
            .endObject();
        String indexName = suffix("test");

        // index data into N-2
        if (isFullyUpgradedTo(VERSION_MINUS_2)) {
            Settings settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

            createIndex(indexName, settings, mapping);

            // MULTIPOINT
            MultiPoint multiPoint = GeometryTestUtils.randomMultiPoint(false);
            indexDoc(indexName, "1", GeoJson.toXContent(multiPoint, jsonBuilder().startObject().field(defaultFieldName), null).endObject());

            // POINT
            Point point = GeometryTestUtils.randomPoint(false);
            indexDoc(indexName, "2", GeoJson.toXContent(point, jsonBuilder().startObject().field(defaultFieldName), null).endObject());
        }

        // regular search on N-1
        if (isFullyUpgradedTo(VERSION_MINUS_1)) {
            assertHitCount(performSearch(indexName, "{}"), 2);
            deleteIndex(indexName);
        }

        // search on searchable snapshot in version N
        if (isFullyUpgradedTo(VERSION_CURRENT)) {
            var mountedIndex = suffix("index-mounted");
            logger.debug("--> mounting index [{}] as [{}]", indexName, mountedIndex);
            mountIndex(repository, snapshot, indexName, randomBoolean(), mountedIndex, IGNORE_WARNINGS.build());
            ensureGreen(mountedIndex);

            assertThat(indexVersion(mountedIndex), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), mountedIndex, 2);
            assertPeerRecoveryVariants(mountedIndex, 2);
        }
    }

    public void testFieldAlias() throws Exception {
        ToXContent mapping = (builder, p) -> builder.startObject("mappings")
            .startObject("properties")
            .startObject(defaultFieldName)
            .field("type", "geo_shape")
            .field("tree", randomBoolean() ? "quadtree" : "geohash")
            .endObject()
            .startObject("alias")
            .field("type", "alias")
            .field("path", defaultFieldName)
            .endObject()
            .endObject()
            .endObject();

        String indexName = suffix("test");

        // index data into N-2
        if (isFullyUpgradedTo(VERSION_MINUS_2)) {
            Settings settings = Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build();

            createIndex(indexName, settings, mapping);

            MultiPoint multiPoint = GeometryTestUtils.randomMultiPoint(false);
            indexDoc(indexName, "1", GeoJson.toXContent(multiPoint, jsonBuilder().startObject().field(defaultFieldName), null).endObject());
        }

        if (isFullyUpgradedTo(VERSION_MINUS_1)) {
            assertHitCount(performSearch(indexName, "{}"), 2);
            deleteIndex(indexName);
        }

        // search on searchable snapshot in version N
        if (isFullyUpgradedTo(VERSION_CURRENT)) {
            var mountedIndex = suffix("index-mounted");
            logger.debug("--> mounting index [{}] as [{}]", indexName, mountedIndex);
            mountIndex(repository, snapshot, indexName, randomBoolean(), mountedIndex, IGNORE_WARNINGS.build());
            ensureGreen(mountedIndex);

            assertThat(indexVersion(mountedIndex), equalTo(VERSION_MINUS_2));
            assertDocCount(client(), mountedIndex, 1);
            assertPeerRecoveryVariants(mountedIndex, 1);
        }
    }

    private void assertPeerRecoveryVariants(String mountedIndex, int expectedDocCount) throws IOException {
        logger.debug("--> adding replica to test peer-recovery");
        var request = newXContentRequest(
            HttpMethod.PUT,
            "/" + mountedIndex + "/_settings",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build()
        );
        request.setOptions(IGNORE_WARNINGS);
        assertOK(client().performRequest(request));
        ensureGreen(mountedIndex);

        logger.debug("--> closing index [{}]", mountedIndex);
        closeIndex(mountedIndex);
        ensureGreen(mountedIndex);

        logger.debug("--> adding replica to test peer-recovery for closed shards");
        request = newXContentRequest(
            HttpMethod.PUT,
            "/" + mountedIndex + "/_settings",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2).build()
        );
        request.setOptions(IGNORE_WARNINGS);
        assertOK(client().performRequest(request));
        ensureGreen(mountedIndex);

        logger.debug("--> re-opening index [{}]", mountedIndex);
        openIndex(mountedIndex);
        ensureGreen(mountedIndex);

        assertDocCount(client(), mountedIndex, expectedDocCount);

        logger.debug("--> deleting index [{}]", mountedIndex);
        deleteIndex(mountedIndex);
    }

    private void assertHitCount(Response response, int expectedHits) throws IOException {
        List<?> hits = (List<?>) (XContentMapValues.extractValue("hits.hits", entityAsMap(response)));
    }

    private Response performSearch(String indexName, String query) throws IOException {
        Request search = new Request("POST", "/" + indexName + "/_search");
        search.setJsonEntity(query);
        return client().performRequest(search);
    }

    static void indexDoc(String index, String docId, XContentBuilder source) throws IOException {
        Request createDoc = new Request("POST", "/" + index + "/_doc/" + docId);
        createDoc.setJsonEntity(Strings.toString(source));
        assertOK(client().performRequest(createDoc));
        refresh(client(), index);
    }

    @Before
    public void beforeTests() throws Exception {
        if (repoRegistered == false) {
            Settings repositorySettings = Settings.builder()
                .put("location", REPOSITORY_PATH.getRoot().toPath().resolve("repo-location").toFile().getPath())
                .build();
            logger.debug("--> registering repository [{}]", repository);
            registerRepository(client(), repository, FsRepository.TYPE, true, repositorySettings);
            repoRegistered = true;
        }

        if (isFullyUpgradedTo(VERSION_MINUS_1) && snapshotCreated == false) {
            logger.debug("--> creating snapshot [{}]", snapshot);
            createSnapshot(client(), repository, snapshot, true);
            snapshotCreated = true;
        }
    }
}
