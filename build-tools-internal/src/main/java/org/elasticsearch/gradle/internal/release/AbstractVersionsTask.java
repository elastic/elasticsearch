/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import org.gradle.api.DefaultTask;
import org.gradle.initialization.layout.BuildLayout;

import java.nio.file.Path;

public abstract class AbstractVersionsTask extends DefaultTask {

    static final String TRANSPORT_VERSION_TYPE = "TransportVersion";
    static final String INDEX_VERSION_TYPE = "IndexVersion";

    static final String SERVER_MODULE_PATH = "server/src/main/java/";
    static final String TRANSPORT_VERSION_FILE_PATH = SERVER_MODULE_PATH + "org/elasticsearch/TransportVersions.java";
    static final String INDEX_VERSION_FILE_PATH = SERVER_MODULE_PATH + "org/elasticsearch/index/IndexVersions.java";

    static final String SERVER_RESOURCES_PATH = "server/src/main/resources/";
    static final String TRANSPORT_VERSIONS_RECORD = SERVER_RESOURCES_PATH + "org/elasticsearch/TransportVersions.csv";
    static final String INDEX_VERSIONS_RECORD = SERVER_RESOURCES_PATH + "org/elasticsearch/index/IndexVersions.csv";

    final Path rootDir;

    protected AbstractVersionsTask(BuildLayout layout) {
        rootDir = layout.getRootDirectory().toPath();
    }

}
