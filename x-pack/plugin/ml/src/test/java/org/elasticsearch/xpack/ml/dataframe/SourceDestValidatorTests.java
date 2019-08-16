/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetection;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_VERSION_CREATED;
import static org.hamcrest.Matchers.equalTo;

public class SourceDestValidatorTests extends ESTestCase {

    private static final String SOURCE_1 = "source-1";
    private static final String SOURCE_2 = "source-2";
    private static final String ALIASED_DEST = "aliased-dest";

    private static final ClusterState CLUSTER_STATE;

    static {
        IndexMetaData source1 = IndexMetaData.builder(SOURCE_1).settings(Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_CREATION_DATE, System.currentTimeMillis()))
            .putAlias(AliasMetaData.builder("source-1-alias").build())
            .build();
        IndexMetaData source2 = IndexMetaData.builder(SOURCE_2).settings(Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_CREATION_DATE, System.currentTimeMillis()))
            .putAlias(AliasMetaData.builder("dest-alias").build())
            .build();
        IndexMetaData aliasedDest = IndexMetaData.builder(ALIASED_DEST).settings(Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(SETTING_CREATION_DATE, System.currentTimeMillis()))
            .putAlias(AliasMetaData.builder("dest-alias").build())
            .build();
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        state.metaData(MetaData.builder()
            .put(IndexMetaData.builder(source1).build(), false)
            .put(IndexMetaData.builder(source2).build(), false)
            .put(IndexMetaData.builder(aliasedDest).build(), false));
        CLUSTER_STATE = state.build();
    }

    public void testCheck_GivenSimpleSourceIndexAndValidDestIndex() {
        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder()
            .setId("test")
            .setSource(createSource("source-1"))
            .setDest(new DataFrameAnalyticsDest("dest", null))
            .setAnalysis(new OutlierDetection())
            .build();

        SourceDestValidator validator = new SourceDestValidator(CLUSTER_STATE, new IndexNameExpressionResolver());
        validator.check(config);
    }

    public void testCheck_GivenMissingConcreteSourceIndex() {
        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder()
            .setId("test")
            .setSource(createSource("missing"))
            .setDest(new DataFrameAnalyticsDest("dest", null))
            .setAnalysis(new OutlierDetection())
            .build();

        SourceDestValidator validator = new SourceDestValidator(CLUSTER_STATE, new IndexNameExpressionResolver());

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> validator.check(config));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("No index matches source index [missing]"));
    }

    public void testCheck_GivenMissingWildcardSourceIndex() {
        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder()
            .setId("test")
            .setSource(createSource("missing*"))
            .setDest(new DataFrameAnalyticsDest("dest", null))
            .setAnalysis(new OutlierDetection())
            .build();

        SourceDestValidator validator = new SourceDestValidator(CLUSTER_STATE, new IndexNameExpressionResolver());

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> validator.check(config));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("No index matches source index [missing*]"));
    }

    public void testCheck_GivenDestIndexSameAsSourceIndex() {
        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder()
            .setId("test")
            .setSource(createSource("source-1"))
            .setDest(new DataFrameAnalyticsDest("source-1", null))
            .setAnalysis(new OutlierDetection())
            .build();

        SourceDestValidator validator = new SourceDestValidator(CLUSTER_STATE, new IndexNameExpressionResolver());

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> validator.check(config));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("Destination index [source-1] must not be included in source index [source-1]"));
    }

    public void testCheck_GivenDestIndexMatchesSourceIndex() {
        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder()
            .setId("test")
            .setSource(createSource("source-*"))
            .setDest(new DataFrameAnalyticsDest(SOURCE_2, null))
            .setAnalysis(new OutlierDetection())
            .build();

        SourceDestValidator validator = new SourceDestValidator(CLUSTER_STATE, new IndexNameExpressionResolver());

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> validator.check(config));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("Destination index [source-2] must not be included in source index [source-*]"));
    }

    public void testCheck_GivenDestIndexMatchesOneOfSourceIndices() {
        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder()
            .setId("test")
            .setSource(createSource("source-1,source-*"))
            .setDest(new DataFrameAnalyticsDest(SOURCE_2, null))
            .setAnalysis(new OutlierDetection())
            .build();

        SourceDestValidator validator = new SourceDestValidator(CLUSTER_STATE, new IndexNameExpressionResolver());

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> validator.check(config));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("Destination index [source-2] must not be included in source index [source-*]"));
    }

    public void testCheck_GivenDestIndexIsAliasThatMatchesMultipleIndices() {
        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder()
            .setId("test")
            .setSource(createSource(SOURCE_1))
            .setDest(new DataFrameAnalyticsDest("dest-alias", null))
            .setAnalysis(new OutlierDetection())
            .build();

        SourceDestValidator validator = new SourceDestValidator(CLUSTER_STATE, new IndexNameExpressionResolver());

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> validator.check(config));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(),
            equalTo("Destination index [dest-alias] should match a single index; matches [source-2, aliased-dest]"));
    }

    public void testCheck_GivenDestIndexIsAliasThatIsIncludedInSource() {
        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder()
            .setId("test")
            .setSource(createSource("source-1"))
            .setDest(new DataFrameAnalyticsDest("source-1-alias", null))
            .setAnalysis(new OutlierDetection())
            .build();

        SourceDestValidator validator = new SourceDestValidator(CLUSTER_STATE, new IndexNameExpressionResolver());

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> validator.check(config));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(),
            equalTo("Destination index [source-1-alias], which is an alias for [source-1], " +
                "must not be included in source index [source-1]"));
    }

    private static DataFrameAnalyticsSource createSource(String... index) {
        return new DataFrameAnalyticsSource(index, null);
    }
}
