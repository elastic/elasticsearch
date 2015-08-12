/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.licenses;

import org.elasticsearch.license.core.License;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;

import java.util.List;

public class LicensesMarvelDoc extends MarvelDoc<LicensesMarvelDoc.Payload> {

    private final Payload payload;

    LicensesMarvelDoc(String index, String type, String id, String clusterName, long timestamp, Payload payload) {
        super(index, type, id, clusterName, timestamp);
        this.payload = payload;
    }

    @Override
    public LicensesMarvelDoc.Payload payload() {
        return payload;
    }

    public static LicensesMarvelDoc createMarvelDoc(String index, String type, String id, String clusterName, long timestamp, String version, List<License> licenses) {
        return new LicensesMarvelDoc(index, type, id, clusterName, timestamp, new Payload(version, licenses));
    }

    public static class Payload {

        private final String version;
        private final List<License> licenses;

        public Payload(String version, List<License> licenses) {
            this.version = version;
            this.licenses = licenses;
        }

        public String getVersion() {
            return version;
        }

        public List<License> getLicenses() {
            return licenses;
        }
    }
}
