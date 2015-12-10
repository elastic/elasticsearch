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
import org.elasticsearch.common.logging.ESLogger;
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
    public static final Version MIN_SUPPORTED_TEMPLATE_VERSION = Version.V_2_0_0_beta2;

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

    public static Version loadDefaultTemplateVersion() {
        return parseTemplateVersion(loadDefaultTemplate());
    }

    public static Version templateVersion(IndexTemplateMetaData templateMetaData) {
        String version = templateMetaData.settings().get("index." + MARVEL_VERSION_FIELD);
        if (Strings.hasLength(version)) {
            try {
                return Version.fromString(version);
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
        return null;
    }

    public static IndexTemplateMetaData findMarvelTemplate(ClusterState state) {
        MetaData metaData = state.getMetaData();
        return metaData != null ? metaData.getTemplates().get(INDEX_TEMPLATE_NAME) : null;
    }

    public static Version parseTemplateVersion(byte[] template) {
        try {
            return VersionUtils.parseVersion(MARVEL_VERSION_FIELD, template);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public static boolean installedTemplateVersionIsSufficient(Version installed) {
        // null indicates couldn't parse the version from the installed template, this means it is probably too old or invalid...
        if (installed == null) {
            return false;
        }
        // ensure the template is not too old
        if (installed.before(MIN_SUPPORTED_TEMPLATE_VERSION)) {
            return false;
        }

        // We do not enforce that versions are equivalent to the current version as we may be in a rolling upgrade scenario
        // and until a master is elected with the new version, data nodes that have been upgraded will not be able to ship
        // data. This means that there is an implication that the new shippers will ship data correctly even with an old template.
        // There is also no upper bound and we rely on elasticsearch nodes not being able to connect to each other across major
        // versions
        return true;
    }

    public static boolean installedTemplateVersionMandatesAnUpdate(Version current, Version installed, ESLogger logger, String exporterName) {
        if (installed == null) {
            logger.debug("exporter [{}] - currently installed marvel template is missing a version - installing a new one [{}]", exporterName, current);
            return true;
        }
        // Never update a very old template
        if (installed.before(MIN_SUPPORTED_TEMPLATE_VERSION)) {
            return false;
        }
        // Always update a template to the last up-to-date version
        if (current.after(installed)) {
            logger.debug("exporter [{}] - currently installed marvel template version [{}] will be updated to a newer version [{}]", exporterName, installed, current);
            return true;
            // When the template is up-to-date, do not update
        } else if (current.equals(installed)) {
            logger.debug("exporter [{}] - currently installed marvel template version [{}] is up-to-date", exporterName, installed);
            return false;
            // Never update a template that is newer than the expected one
        } else {
            logger.debug("exporter [{}] - currently installed marvel template version [{}] is newer than the one required [{}]... keeping it.", exporterName, installed, current);
            return false;
        }
    }
}
