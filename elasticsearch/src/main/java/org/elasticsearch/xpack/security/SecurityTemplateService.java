/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xpack.template.TemplateUtils;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * SecurityTemplateService is responsible for adding the template needed for the
 * {@code .security} administrative index.
 */
public class SecurityTemplateService extends AbstractComponent implements ClusterStateListener {

    public static final String SECURITY_INDEX_NAME = ".security";
    public static final String SECURITY_TEMPLATE_NAME = "security-index-template";
    private static final String SECURITY_VERSION_STRING = "security-version";
    static final String SECURITY_INDEX_TEMPLATE_VERSION_PATTERN = Pattern.quote("${security.template.version}");

    private final InternalClient client;
    final AtomicBoolean templateCreationPending = new AtomicBoolean(false);
    final AtomicBoolean updateMappingPending = new AtomicBoolean(false);

    public SecurityTemplateService(Settings settings, ClusterService clusterService,
                                   InternalClient client) {
        super(settings);
        this.client = client;
        clusterService.add(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false) {
            return;
        }
        ClusterState state = event.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think may not have .security-audit-
            // but they may not have been restored from the cluster state on disk
            logger.debug("template service waiting until state has been recovered");
            return;
        }
        if (securityTemplateExistsAndIsUpToDate(state, logger) == false) {
            updateSecurityTemplate();
        }
        // make sure mapping is up to date
        if (state.metaData().getIndices() != null) {
            if (securityIndexMappingUpToDate(state, logger) == false) {
                updateSecurityMapping();
            }
        }
    }

    private void updateSecurityTemplate() {
        // only put the template if this is not already in progress
        if (templateCreationPending.compareAndSet(false, true)) {
            putSecurityTemplate();
        }
    }

    private void updateSecurityMapping() {
        // only update the mapping if this is not already in progress
        if (updateMappingPending.compareAndSet(false, true) ) {
            putSecurityMappings();
        }
    }

    private void putSecurityMappings() {
        String template = TemplateUtils.loadTemplate("/" + SECURITY_TEMPLATE_NAME + ".json", Version.CURRENT.toString()
                , SECURITY_INDEX_TEMPLATE_VERSION_PATTERN);
        Map<String, Object> typeMappingMap;
        try {
            XContentParser xParser = XContentFactory.xContent(template).createParser(template);
            typeMappingMap = xParser.map();
        } catch (IOException e) {
            updateMappingPending.set(false);
            logger.error("failed to parse the security index template", e);
            throw new ElasticsearchException("failed to parse the security index template", e);
        }

        // here go over all types found in the template and update them
        // we need to wait for all types
        final Map<String, PutMappingResponse> updateResults = ConcurrentCollections.newConcurrentMap();
        @SuppressWarnings("unchecked")
        Map<String, Object> typeMappings = (Map<String, Object>) typeMappingMap.get("mappings");
        int expectedResults = typeMappings.size();
        for (String type : typeMappings.keySet()) {
            // get the mappings from the template definition
            @SuppressWarnings("unchecked")
            Map<String, Object> typeMapping = (Map<String, Object>) typeMappings.get(type);
            // update the mapping
            putSecurityMapping(updateResults, expectedResults, type, typeMapping);
        }
    }

    private void putSecurityMapping(final Map<String, PutMappingResponse> updateResults, int expectedResults,
                                    final String type, Map<String, Object> typeMapping) {
        logger.debug("updating mapping of the security index for type [{}]", type);
        PutMappingRequest putMappingRequest = client.admin().indices()
                .preparePutMapping(SECURITY_INDEX_NAME).setSource(typeMapping).setType(type).request();
        client.admin().indices().putMapping(putMappingRequest, new ActionListener<PutMappingResponse>() {
            @Override
            public void onResponse(PutMappingResponse putMappingResponse) {
                if (putMappingResponse.isAcknowledged() == false) {
                    updateMappingPending.set(false);
                    throw new ElasticsearchException("update mapping for [{}] security index " +
                            "was not acknowledged", type);
                } else {
                    updateResults.put(type, putMappingResponse);
                    if (updateResults.size() == expectedResults) {
                        updateMappingPending.set(false);
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                updateMappingPending.set(false);
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to update mapping for [{}] on security index", type), e);
            }
        });
    }

    private void putSecurityTemplate() {
        logger.debug("putting the security index template");
        String template = TemplateUtils.loadTemplate("/" + SECURITY_TEMPLATE_NAME + ".json", Version.CURRENT.toString()
                , SECURITY_INDEX_TEMPLATE_VERSION_PATTERN);

        PutIndexTemplateRequest putTemplateRequest = client.admin().indices()
                .preparePutTemplate(SECURITY_TEMPLATE_NAME).setSource(template).request();
        client.admin().indices().putTemplate(putTemplateRequest, new ActionListener<PutIndexTemplateResponse>() {
            @Override
            public void onResponse(PutIndexTemplateResponse putIndexTemplateResponse) {
                templateCreationPending.set(false);
                if (putIndexTemplateResponse.isAcknowledged() == false) {
                    throw new ElasticsearchException("put template for security index was not acknowledged");
                }
            }

            @Override
            public void onFailure(Exception e) {
                templateCreationPending.set(false);
                logger.warn("failed to put security index template", e);
            }
        });
    }

    static boolean securityIndexMappingUpToDate(ClusterState clusterState, Logger logger) {
        IndexMetaData indexMetaData = clusterState.metaData().getIndices().get(SECURITY_INDEX_NAME);
        if (indexMetaData != null) {
            for (Object object : indexMetaData.getMappings().values().toArray()) {
                MappingMetaData mappingMetaData = (MappingMetaData) object;
                if (mappingMetaData.type().equals(MapperService.DEFAULT_MAPPING)) {
                    continue;
                }
                try {
                    if (containsCorrectVersion(mappingMetaData.sourceAsMap()) == false) {
                        return false;
                    }
                } catch (IOException e) {
                    logger.error("Cannot parse the mapping for security index.", e);
                    throw new ElasticsearchException("Cannot parse the mapping for security index.", e);
                }
            }
            return true;
        } else {
            // index does not exist so when we create it it will be up to date
            return true;
        }
    }

    static boolean securityTemplateExistsAndIsUpToDate(ClusterState state, Logger logger) {
        IndexTemplateMetaData templateMeta = state.metaData().templates().get(SECURITY_TEMPLATE_NAME);
        if (templateMeta == null) {
            return false;
        }
        ImmutableOpenMap<String, CompressedXContent> mappings = templateMeta.getMappings();
        // check all mappings contain correct version in _meta
        // we have to parse the source here which is annoying
        for (Object typeMapping : mappings.values().toArray()) {
            CompressedXContent typeMappingXContent = (CompressedXContent) typeMapping;
            try (XContentParser xParser = XContentFactory.xContent(typeMappingXContent.toString())
                    .createParser(typeMappingXContent.toString())) {
                Map<String, Object> typeMappingMap = xParser.map();
                // should always contain one entry with key = typename
                assert (typeMappingMap.size() == 1);
                String key = typeMappingMap.keySet().iterator().next();
                // get the actual mapping entries
                @SuppressWarnings("unchecked")
                Map<String, Object> mappingMap = (Map<String, Object>) typeMappingMap.get(key);
                if (containsCorrectVersion(mappingMap) == false) {
                    return false;
                }
            } catch (IOException e) {
                logger.error("Cannot parse the template for security index.", e);
                throw new IllegalStateException("Cannot parse the template for security index.", e);
            }
        }
        return true;
    }

    private static boolean containsCorrectVersion(Map<String, Object> typeMappingMap) {
        @SuppressWarnings("unchecked")
        Map<String, Object> meta = (Map<String, Object>) typeMappingMap.get("_meta");
        if (meta == null) {
            // pre 5.0, cannot be up to date
            return false;
        }
        if (Version.CURRENT.toString().equals(meta.get(SECURITY_VERSION_STRING)) == false) {
            // wrong version
            return false;
        }
        return true;
    }

    public static boolean securityIndexMappingAndTemplateUpToDate(ClusterState clusterState, Logger logger) {
        if (SecurityTemplateService.securityTemplateExistsAndIsUpToDate(clusterState, logger) == false) {
            logger.debug("security template [{}] does not exist or is not up to date, so service cannot start",
                    SecurityTemplateService.SECURITY_TEMPLATE_NAME);
            return false;
        }
        if (SecurityTemplateService.securityIndexMappingUpToDate(clusterState, logger) == false) {
            logger.debug("mapping for security index not up to date, so service cannot start");
            return false;
        }
        return true;
    }
}
