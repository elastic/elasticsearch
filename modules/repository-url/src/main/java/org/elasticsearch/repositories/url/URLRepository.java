/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.url;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.url.URLBlobStore;
import org.elasticsearch.common.blobstore.url.http.URLHttpClient;
import org.elasticsearch.common.blobstore.url.http.URLHttpClientSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.URIPattern;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collections;
import java.util.List;

/**
 * Read-only URL-based implementation of the BlobStoreRepository
 * <p>
 * This repository supports the following settings
 * <dl>
 * <dt>{@code url}</dt><dd>URL to the root of repository. This is mandatory parameter.</dd>
 * <dt>{@code concurrent_streams}</dt><dd>Number of concurrent read/write stream (per repository on each node). Defaults to 5.</dd>
 * </dl>
 */
public class URLRepository extends BlobStoreRepository {
    private static final Logger logger = LogManager.getLogger(URLRepository.class);

    public static final String TYPE = "url";

    static {
        // noinspection ConstantConditions
        assert TYPE.equals(BlobStoreRepository.URL_REPOSITORY_TYPE);
    }

    public static final Setting<List<String>> SUPPORTED_PROTOCOLS_SETTING = Setting.stringListSetting(
        "repositories.url.supported_protocols",
        List.of("http", "https", "ftp", "file", "jar"),
        Property.NodeScope
    );

    public static final Setting<List<URIPattern>> ALLOWED_URLS_SETTING = Setting.listSetting(
        "repositories.url.allowed_urls",
        Collections.emptyList(),
        URIPattern::new,
        Property.NodeScope
    );

    public static final Setting<URL> URL_SETTING = new Setting<>("url", "http:", URLRepository::parseURL, Property.NodeScope);
    public static final Setting<URL> REPOSITORIES_URL_SETTING = new Setting<>(
        "repositories.url.url",
        (s) -> s.get("repositories.uri.url", "http:"),
        URLRepository::parseURL,
        Property.NodeScope
    );

    private final List<String> supportedProtocols;

    private final URIPattern[] urlWhiteList;

    private final Environment environment;

    private final URL url;

    private final URLHttpClient httpClient;

    private final URLHttpClientSettings httpClientSettings;

    /**
     * Constructs a read-only URL-based repository
     */
    public URLRepository(
        ProjectId projectId,
        RepositoryMetadata metadata,
        Environment environment,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        BigArrays bigArrays,
        RecoverySettings recoverySettings,
        URLHttpClient.Factory httpClientFactory
    ) {
        super(projectId, metadata, namedXContentRegistry, clusterService, bigArrays, recoverySettings, BlobPath.EMPTY);

        if (URL_SETTING.exists(metadata.settings()) == false && REPOSITORIES_URL_SETTING.exists(environment.settings()) == false) {
            throw new RepositoryException(metadata.name(), "missing url");
        }
        this.environment = environment;
        supportedProtocols = SUPPORTED_PROTOCOLS_SETTING.get(environment.settings());
        urlWhiteList = ALLOWED_URLS_SETTING.get(environment.settings()).toArray(new URIPattern[] {});
        url = URL_SETTING.exists(metadata.settings())
            ? URL_SETTING.get(metadata.settings())
            : REPOSITORIES_URL_SETTING.get(environment.settings());

        this.httpClientSettings = URLHttpClientSettings.fromSettings(metadata.settings());
        this.httpClient = httpClientFactory.create(httpClientSettings);
    }

    @Override
    protected BlobStore createBlobStore() {
        URL normalizedURL = checkURL(url);
        return new URLBlobStore(environment.settings(), normalizedURL, httpClient, httpClientSettings);
    }

    // only use for testing
    @Override
    protected BlobContainer blobContainer() {
        return super.blobContainer();
    }

    // only use for testing
    @Override
    protected BlobStore getBlobStore() {
        return super.getBlobStore();
    }

    /**
     * Makes sure that the url is white listed or if it points to the local file system it matches one on of the root path in path.repo
     */
    private URL checkURL(URL urlToCheck) {
        String protocol = urlToCheck.getProtocol();
        if (protocol == null) {
            throw new RepositoryException(getMetadata().name(), "unknown url protocol from URL [" + urlToCheck + "]");
        }
        for (String supportedProtocol : supportedProtocols) {
            if (supportedProtocol.equals(protocol)) {
                try {
                    if (URIPattern.match(urlWhiteList, urlToCheck.toURI())) {
                        // URL matches white list - no additional processing is needed
                        return urlToCheck;
                    }
                } catch (URISyntaxException ex) {
                    logger.warn("cannot parse the specified url [{}]", urlToCheck);
                    throw new RepositoryException(getMetadata().name(), "cannot parse the specified url [" + urlToCheck + "]");
                }
                // We didn't match white list - try to resolve against path.repo
                URL normalizedUrl = environment.resolveRepoURL(urlToCheck);
                if (normalizedUrl == null) {
                    String logMessage = "The specified url [{}] doesn't start with any repository paths specified by the "
                        + "path.repo setting or by {} setting: [{}] ";
                    logger.warn(logMessage, urlToCheck, ALLOWED_URLS_SETTING.getKey(), environment.repoDirs());
                    String exceptionMessage = "file url ["
                        + urlToCheck
                        + "] doesn't match any of the locations specified by path.repo or "
                        + ALLOWED_URLS_SETTING.getKey();
                    throw new RepositoryException(getMetadata().name(), exceptionMessage);
                }
                return normalizedUrl;
            }
        }
        throw new RepositoryException(getMetadata().name(), "unsupported url protocol [" + protocol + "] from URL [" + urlToCheck + "]");
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    private static URL parseURL(String s) {
        try {
            return new URL(s);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Unable to parse URL repository setting", e);
        }
    }

    @Override
    protected void doClose() {
        IOUtils.closeWhileHandlingException(httpClient);
        super.doClose();
    }
}
