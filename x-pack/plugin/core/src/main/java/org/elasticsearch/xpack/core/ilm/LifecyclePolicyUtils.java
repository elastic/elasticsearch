/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ItemUsage;
import org.elasticsearch.cluster.metadata.MetadataIndexTemplateService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.Streams;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A utility class used for index lifecycle policies
 */
public class LifecyclePolicyUtils {

    private LifecyclePolicyUtils() {};

    /**
     * Loads a built-in index lifecycle policy and returns its source.
     */
    public static LifecyclePolicy loadPolicy(String name, String resource, NamedXContentRegistry xContentRegistry) {
        try {
            BytesReference source = load(resource);
            validate(source);

            try (XContentParser parser = XContentType.JSON.xContent()
                .createParser(xContentRegistry, LoggingDeprecationHandler.THROW_UNSUPPORTED_OPERATION, source.utf8ToString())) {
                LifecyclePolicy policy = LifecyclePolicy.parse(parser, name);
                policy.validate();
                return policy;
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("unable to load policy [" + name + "] from [" + resource + "]", e);
        }
    }

    /**
     * Loads a resource from the classpath and returns it as a {@link BytesReference}
     */
    private static BytesReference load(String name) throws IOException {
        try (InputStream is = LifecyclePolicyUtils.class.getResourceAsStream(name)) {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                Streams.copy(is, out);
                return new BytesArray(out.toByteArray());
            }
        }
    }

    /**
     * Parses and validates that the source is not empty.
     */
    private static void validate(BytesReference source) {
        if (source == null) {
            throw new ElasticsearchParseException("policy must not be null");
        }

        try {
            XContentHelper.convertToMap(source, false, XContentType.JSON).v2();
        } catch (NotXContentException e) {
            throw new ElasticsearchParseException("policy must not be empty");
        } catch (Exception e) {
            throw new ElasticsearchParseException("invalid policy", e);
        }
    }

    /**
     * Given a cluster state and ILM policy, calculate the {@link ItemUsage} of
     * the policy (what indices, data streams, and templates use the policy)
     */
    public static ItemUsage calculateUsage(final IndexNameExpressionResolver indexNameExpressionResolver,
                                           final ClusterState state, final String policyName) {
        final List<String> indices = StreamSupport.stream(state.metadata().indices().values().spliterator(), false)
            .map(cursor -> cursor.value)
            .filter(indexMetadata -> policyName.equals(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexMetadata.getSettings())))
            .map(indexMetadata -> indexMetadata.getIndex().getName())
            .collect(Collectors.toList());

        final List<String> allDataStreams = indexNameExpressionResolver.dataStreamNames(state,
            IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN);

        final List<String> dataStreams = allDataStreams.stream()
            .filter(dsName -> {
                String indexTemplate = MetadataIndexTemplateService.findV2Template(state.metadata(), dsName, false);
                if (indexTemplate != null) {
                    Settings settings = MetadataIndexTemplateService.resolveSettings(state.metadata(), indexTemplate);
                    return policyName.equals(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(settings));
                } else {
                    return false;
                }
            })
            .collect(Collectors.toList());

        final List<String> composableTemplates = state.metadata().templatesV2().keySet().stream()
            .filter(templateName -> {
                Settings settings = MetadataIndexTemplateService.resolveSettings(state.metadata(), templateName);
                return policyName.equals(LifecycleSettings.LIFECYCLE_NAME_SETTING.get(settings));
            })
            .collect(Collectors.toList());

        return new ItemUsage(indices, dataStreams, composableTemplates);
    }
}
