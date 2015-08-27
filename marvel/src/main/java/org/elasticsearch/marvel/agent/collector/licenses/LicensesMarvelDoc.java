/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.licenses;

import org.elasticsearch.license.core.License;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;

import java.util.List;

public class LicensesMarvelDoc extends MarvelDoc {

    private final String clusterName;
    private final String version;
    private final List<License> licenses;

    LicensesMarvelDoc(String index, String type, String id, String clusterUUID, long timestamp,
                      String clusterName, String version, List<License> licenses) {
        super(index, type, id, clusterUUID, timestamp);
        this.clusterName = clusterName;
        this.version = version;
        this.licenses = licenses;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getVersion() {
        return version;
    }

    public List<License> getLicenses() {
        return licenses;
    }

}
