/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.allocation;

import co.elastic.elasticsearch.serverless.constants.ProjectType;
import co.elastic.elasticsearch.stateless.Stateless;

import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static java.lang.Integer.parseInt;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.PER_INDEX_MAX_NUMBER_OF_SHARDS;

public class StatelessIndexSettingProvider implements IndexSettingProvider {
    private static final Logger logger = LogManager.getLogger(StatelessIndexSettingProvider.class);

    /**
     * Sets the number of shards that a new index will have, absent the create index request specifying the number of shards.
     * This setting is not applied to indices backing data streams, nor internal system indices.
     * Values > 0 will be used to override the {@code ServerlessSharedSettings.ProjectType} shard number default.
     * The default of 0 will indicate that this setting is inactive.
     * NB that setting 'null' is equivalent to 0.
     */
    public static final Setting<Integer> DEFAULT_NUMBER_OF_SHARDS_FOR_REGULAR_INDICES_SETTING = Setting.intSetting(
        "serverless.indices.regular.default_number_of_shards",
        0,
        0,
        parseInt(PER_INDEX_MAX_NUMBER_OF_SHARDS),
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );
    private volatile int defaultNumberOfShardsForRegularIndices = 0;

    private final ProjectType projectType;
    private Predicate<String> systemNamePredicate;

    public StatelessIndexSettingProvider(ProjectType projectType) {
        this.projectType = projectType;
    }

    public void initialize(ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver) {
        clusterService.getClusterSettings()
            .initializeAndWatch(
                DEFAULT_NUMBER_OF_SHARDS_FOR_REGULAR_INDICES_SETTING,
                updatedValue -> defaultNumberOfShardsForRegularIndices = updatedValue
            );
        final var systemNameRunAutomaton = new CharacterRunAutomaton(indexNameExpressionResolver.getSystemNameAutomaton());
        systemNamePredicate = systemNameRunAutomaton::run;
    }

    @Override
    public Settings getAdditionalIndexSettings(
        String indexName,
        @Nullable String dataStreamName,
        boolean isTimeSeries,
        Metadata metadata,
        Instant resolvedAt,
        Settings indexTemplateAndCreateRequestSettings,
        List<CompressedXContent> combinedTemplateMappings
    ) {
        assert systemNamePredicate != null : "object is not initialized properly";
        Settings.Builder settings = Settings.builder();
        // TODO find a prover way to bypass index template validation
        if (Objects.equals(indexName, "validate-index-name") == false) {
            settings.put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), Stateless.NAME);
        }

        if (dataStreamName == null
            && false == INDEX_NUMBER_OF_SHARDS_SETTING.exists(indexTemplateAndCreateRequestSettings)
            && false == systemNamePredicate.test(indexName)) {
            int defaultShards = defaultNumberOfShardsForRegularIndices > 0
                ? defaultNumberOfShardsForRegularIndices
                : projectType.getNumberOfShards();
            logger.debug(
                () -> Strings.format(
                    "Stateless default number of shards is [%d], applying to index [%s]. Project type [%s], project default shards [%d], "
                        + "%s override (with value [%d]) %s active",
                    defaultShards,
                    indexName,
                    projectType,
                    projectType.getNumberOfShards(),
                    DEFAULT_NUMBER_OF_SHARDS_FOR_REGULAR_INDICES_SETTING.getKey(),
                    defaultNumberOfShardsForRegularIndices,
                    (defaultNumberOfShardsForRegularIndices > 0 ? "is" : "is not")
                )
            );
            settings.put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), defaultShards);
        }
        return settings.build();
    }
}
