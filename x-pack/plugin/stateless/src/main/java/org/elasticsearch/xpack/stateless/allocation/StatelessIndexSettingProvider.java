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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;

public class StatelessIndexSettingProvider implements IndexSettingProvider {
    private static final Logger logger = LogManager.getLogger(StatelessIndexSettingProvider.class);

    private final ProjectType projectType;
    private Predicate<String> systemNamePredicate;

    public StatelessIndexSettingProvider(ProjectType projectType) {
        this.projectType = projectType;
    }

    public void initialize(IndexNameExpressionResolver indexNameExpressionResolver) {
        final var systemNameRunAutomaton = new CharacterRunAutomaton(indexNameExpressionResolver.getSystemNameAutomaton());
        systemNamePredicate = systemNameRunAutomaton::run;
    }

    @Override
    public Settings getAdditionalIndexSettings(
        String indexName,
        String dataStreamName,
        boolean timeSeries,
        Metadata metadata,
        Instant resolvedAt,
        Settings allSettings,
        List<CompressedXContent> combinedTemplateMappings
    ) {
        assert systemNamePredicate != null : "object is not initialized properly";
        Settings.Builder settings = Settings.builder();
        // TODO find a prover way to bypass index template validation
        if (Objects.equals(indexName, "validate-index-name") == false) {
            settings.put(ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING.getKey(), Stateless.NAME);
        }

        if (dataStreamName == null
            && false == INDEX_NUMBER_OF_SHARDS_SETTING.exists(allSettings)
            && false == systemNamePredicate.test(indexName)) {
            logger.debug(
                () -> Strings.format(
                    "apply stateless number_of_shards default of [%d] for project type [%s] to index [%s]",
                    projectType.getNumberOfShards(),
                    projectType,
                    indexName
                )
            );
            settings.put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), projectType.getNumberOfShards());
        }
        return settings.build();
    }
}
