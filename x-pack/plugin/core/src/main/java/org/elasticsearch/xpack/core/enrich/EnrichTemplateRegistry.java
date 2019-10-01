package org.elasticsearch.xpack.core.enrich;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.ENRICH_ORIGIN;
import static org.elasticsearch.xpack.core.XPackSettings.ENRICH_ENABLED_SETTING;

public class EnrichTemplateRegistry extends IndexTemplateRegistry {
    // history (please add a comment why you increased the version here)
    // version 1: initial
    public static final String INDEX_TEMPLATE_VERSION = "1";

    public static final String ENRICH_TEMPLATE_VERSION_VARIABLE = "xpack.enrich.template.version";
    public static final String ENRICH_TEMPLATE_NAME = ".enrich";

    public static final IndexTemplateConfig TEMPLATE_ENRICH = new IndexTemplateConfig(
        ENRICH_TEMPLATE_NAME,
        "/enrich.json",
        INDEX_TEMPLATE_VERSION,
        ENRICH_TEMPLATE_VERSION_VARIABLE
    );

    private final Boolean enrichEnabled;

    public EnrichTemplateRegistry(Settings nodeSettings, ClusterService clusterService, ThreadPool threadPool,
                                  Client client, NamedXContentRegistry xContentRegistry) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
        enrichEnabled = ENRICH_ENABLED_SETTING.get(nodeSettings);
    }

    @Override
    protected List<IndexTemplateConfig> getTemplateConfigs() {
        if (enrichEnabled == false) {
            return Collections.emptyList();
        }
        return Collections.singletonList(TEMPLATE_ENRICH);
    }

    @Override
    protected List<LifecyclePolicyConfig> getPolicyConfigs() {
        return Collections.emptyList();
    }

    @Override
    protected String getOrigin() {
        return ENRICH_ORIGIN;
    }
}
