/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.marvel.support.VersionUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public final class MarvelTemplateUtils {

    public static final String MARVEL_TEMPLATE_FILE = "/marvel_index_template.json";
    public static final String INDEX_TEMPLATE_NAME = ".marvel-es";
    public static final String MARVEL_VERSION_FIELD = "marvel_version";


    private MarvelTemplateUtils() {
    }

    /**
     * Loads the default Marvel template
     */
    public static byte[] loadDefaultTemplate() {
        try (InputStream is = MarvelTemplateUtils.class.getResourceAsStream(MARVEL_TEMPLATE_FILE)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(is, out);
            return out.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("unable to load marvel template", e);
        }
    }

    public static Version templateVersion(IndexTemplateMetaData templateMetaData) {
        String version = templateMetaData.settings().get("index." + MARVEL_VERSION_FIELD);
        if (Strings.hasLength(version)) {
            return Version.fromString(version);
        }
        return null;
    }

    public static IndexTemplateMetaData findMarvelTemplate(ClusterState state) {
        MetaData metaData = state.getMetaData();
        return metaData != null ? metaData.getTemplates().get(INDEX_TEMPLATE_NAME) : null;
    }

    public static Version parseTemplateVersion(byte[] template) {
        return VersionUtils.parseVersion(MARVEL_VERSION_FIELD, template);
    }

    public static Version parseTemplateVersion(String template) {
        return VersionUtils.parseVersion(MARVEL_VERSION_FIELD, template);
    }
}
