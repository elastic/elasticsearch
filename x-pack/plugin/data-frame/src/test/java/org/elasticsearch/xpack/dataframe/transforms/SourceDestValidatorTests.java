/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.dataframe.transforms;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DestConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.SourceConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.pivot.PivotConfigTests;

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
        DataFrameTransformConfig config = createDataFrameTransform(new SourceConfig(SOURCE_1), new DestConfig("dest", null));
        SourceDestValidator.validate(config, CLUSTER_STATE, new IndexNameExpressionResolver(), false);
    }

    public void testCheck_GivenMissingConcreteSourceIndex() {
        DataFrameTransformConfig config = createDataFrameTransform(new SourceConfig("missing"), new DestConfig("dest", null));

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> SourceDestValidator.validate(config, CLUSTER_STATE, new IndexNameExpressionResolver(), false));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("Source index [missing] does not exist"));
        SourceDestValidator.validate(config, CLUSTER_STATE, new IndexNameExpressionResolver(), true);
    }

    public void testCheck_GivenMissingWildcardSourceIndex() {
        DataFrameTransformConfig config = createDataFrameTransform(new SourceConfig("missing*"), new DestConfig("dest", null));

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> SourceDestValidator.validate(config, CLUSTER_STATE, new IndexNameExpressionResolver(), false));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("Source index [missing*] does not exist"));
        SourceDestValidator.validate(config, CLUSTER_STATE, new IndexNameExpressionResolver(), true);
    }

    public void testCheck_GivenDestIndexSameAsSourceIndex() {
        DataFrameTransformConfig config = createDataFrameTransform(new SourceConfig(SOURCE_1), new DestConfig("source-1", null));

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> SourceDestValidator.validate(config, CLUSTER_STATE, new IndexNameExpressionResolver(), false));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("Destination index [source-1] is included in source expression [source-1]"));
        SourceDestValidator.validate(config, CLUSTER_STATE, new IndexNameExpressionResolver(), true);
    }

    public void testCheck_GivenDestIndexMatchesSourceIndex() {
        DataFrameTransformConfig config = createDataFrameTransform(new SourceConfig("source-*"), new DestConfig(SOURCE_2, null));

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> SourceDestValidator.validate(config, CLUSTER_STATE, new IndexNameExpressionResolver(), false));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("Destination index [source-2] is included in source expression [source-*]"));
        SourceDestValidator.validate(config, CLUSTER_STATE, new IndexNameExpressionResolver(), true);
    }

    public void testCheck_GivenDestIndexMatchesOneOfSourceIndices() {
        DataFrameTransformConfig config = createDataFrameTransform(new SourceConfig("source-1", "source-*"),
            new DestConfig(SOURCE_2, null));

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> SourceDestValidator.validate(config, CLUSTER_STATE, new IndexNameExpressionResolver(), false));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("Destination index [source-2] is included in source expression [source-*]"));
        SourceDestValidator.validate(config, CLUSTER_STATE, new IndexNameExpressionResolver(), true);
    }

    public void testCheck_GivenDestIndexIsAliasThatMatchesMultipleIndices() {
        DataFrameTransformConfig config = createDataFrameTransform(new SourceConfig(SOURCE_1), new DestConfig("dest-alias", null));

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> SourceDestValidator.validate(config, CLUSTER_STATE, new IndexNameExpressionResolver(), false));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(),
            equalTo("Destination index [dest-alias] should refer to a single index"));

        e = expectThrows(ElasticsearchStatusException.class,
            () -> SourceDestValidator.validate(config, CLUSTER_STATE, new IndexNameExpressionResolver(), true));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(),
            equalTo("Destination index [dest-alias] should refer to a single index"));
    }

    public void testCheck_GivenDestIndexIsAliasThatIsIncludedInSource() {
        DataFrameTransformConfig config = createDataFrameTransform(new SourceConfig(SOURCE_1), new DestConfig("source-1-alias", null));

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> SourceDestValidator.validate(config, CLUSTER_STATE, new IndexNameExpressionResolver(), false));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(),
            equalTo("Destination index [source-1] is included in source expression [source-1]"));

        SourceDestValidator.validate(config, CLUSTER_STATE, new IndexNameExpressionResolver(), true);
    }

    private static DataFrameTransformConfig createDataFrameTransform(SourceConfig sourceConfig, DestConfig destConfig) {
        return new DataFrameTransformConfig("test",
            sourceConfig,
            destConfig,
            TimeValue.timeValueSeconds(60),
            null,
            null,
            PivotConfigTests.randomPivotConfig(),
            null);
    }
}
